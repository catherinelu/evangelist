package main

import (
  "fmt"
  "net/http"
  "io"
  "os"
  "os/exec"
  "sync"
  "strconv"
  "strings"
  "math"
  "errors"
  "crypto/rand"
  "launchpad.net/goamz/aws"
  "launchpad.net/goamz/s3"
)

// allow at most 1 MB of form data to be passed to the server
const MAX_MULTIPART_FORM_BYTES = 1024 * 1024;

// number of workers to run simultaneously to convert a PDF
const NUM_WORKERS = 2;

// possible alpha numeric characters
const ALPHA_NUMERIC = "abcdefghijklmnopqrstuvwxyz0123456789"

/* If `err` is non-nil, write a 500 error to `writer. Otherwise, do nothing.
 * Returns true if there was an error or false otherwise. */
func handleError(err error, writer http.ResponseWriter) bool {
  if err != nil {
    fmt.Printf(err.Error())
    http.Error(writer, err.Error(), http.StatusInternalServerError)
    return true
  }

  return false
}

/* Returns the number of pages in the PDF specified by `pdfPath`. */
func getNumPages(pdfPath string) (int, error) {
  // ghostscript can retrieve us the number of pages
  cmd := exec.Command("gs", "-q", "-dNODISPLAY", "-c",
    fmt.Sprintf("(%s) (r) file runpdfbegin pdfpagecount = quit", pdfPath))
  numPagesBytes, err := cmd.Output()

  // convert []byte -> string -> int (painful, but necessary)
  if err != nil { return -1, err }
  numPagesStr := strings.Trim(string(numPagesBytes), "\n")
  numPagesInt64, err := strconv.ParseInt(numPagesStr, 10, 0)

  if err != nil { return -1, err }
  return int(numPagesInt64), nil
}

/* Converts the PDF at `pdfPath` to JPEGs. Outputs the JPEGs to the provided
 * `jpegPath` (note: '%d' in `jpegPath` will be replaced by the JPEG
 * number). Converts pages within the range [`firstPage`, `lastPage`]. Calls
 * `wg.Done()` once finished. */
func convertPagesToJPEGs(wg *sync.WaitGroup, pdfPath string, jpegPath string,
    firstPage int, lastPage int) {
  defer wg.Done()

  outputFileOption := fmt.Sprintf("-sOutputFile=%s", jpegPath)
  firstPageOption := fmt.Sprintf("-dFirstPage=%d", firstPage)
  lastPageOption := fmt.Sprintf("-dLastPage=%d", lastPage)

  // use ghostscript for PDF -> JPEG conversion at 300 density
  cmd := exec.Command("gs", "-dNOPAUSE", "-sDEVICE=jpeg", firstPageOption,
    lastPageOption, outputFileOption, "-dJPEGQ=90", "-r300", "-q", pdfPath,
    "-c", "quit")
  cmd.Run()
}

/* Generates and returns a random string of the given length. */
func generateRandomString(length int) string {
  bytes := make([]byte, length)
  rand.Read(bytes)

  for i, randomByte := range bytes {
    // index randomly into a list of alpha numeric characters
    index := randomByte % byte(len(ALPHA_NUMERIC))
    bytes[i] = ALPHA_NUMERIC[index]
  }

  return string(bytes)
}

/* Converts the PDF at `pdfPath` to JPEGs. Outputs the JPEGs to the provided
 * `jpegPath` (note: '%d' in `jpegPath` will be replaced by the JPEG
 * number). */
func convertPDFToJPEGs(pdfPath string, jpegPath string) error {
  numPages, err := getNumPages(pdfPath)
  if err != nil { return err }

  // find number of pages to convert per worker
  numPagesPerWorkerFloat64 := float64(numPages) / float64(NUM_WORKERS)
  numPagesPerWorker := int(math.Ceil(numPagesPerWorkerFloat64))

  var wg sync.WaitGroup
  for firstPage := 1; firstPage <= numPages;
      firstPage = firstPage + numPagesPerWorker {
    // spawn workers, keeping track of them to wait until they're finished
    wg.Add(1)
    lastPage := firstPage + numPagesPerWorker - 1
    if lastPage > numPages {
      lastPage = numPages
    }

    go convertPagesToJPEGs(&wg, pdfPath, "/tmp/pages/page%d.jpg",
      firstPage, lastPage)
  }

  wg.Wait()
  return nil
}

/* Finds the PDF the user would like to convert. Downloads it to a temporary
 * file for processing. Returns the temporary file path. */
func fetchPDF(request *http.Request, bucket *s3.Bucket) (string, error) {
  err := request.ParseMultipartForm(MAX_MULTIPART_FORM_BYTES)
  if err != nil { return "", err }

  s3PDFPathSet, ok := request.Form["pdf"]

  // ensure user gives us precisely one PDF to convert
  if !ok {
    err = errors.New("Must specify a PDF to convert in the 'pdf' key.\n")
    return "", err
  }

  if len(s3PDFPathSet) != 1 {
    err = errors.New("Must specify exactly one S3 PDF path in 'pdf' key.\n")
    return "", err
  }

  // find PDF in S3
  s3PDFPath := request.Form["pdf"][0]
  fmt.Printf("Accessing bucket %s\n", s3PDFPath)
  reader, err := bucket.GetReader(s3PDFPath)

  if err != nil { return "", err }
  defer reader.Close()

  // copy multipart data into temporary file for processing
  pdfPath := "/tmp/" + generateRandomString(50) + ".pdf"
  fmt.Printf("PDF path is %s\n", pdfPath)
  pdf, err := os.Create(pdfPath)

  if err != nil { return "", err }
  defer pdf.Close()

  _, err = io.Copy(pdf, reader)
  if err != nil { return "", err }

  return pdfPath, nil
}

/* Returns an S3 connection to the given bucket. */
func connectToS3(bucketName string) (*s3.Bucket, error) {
  auth, err := aws.EnvAuth()
  if err != nil { return nil, err }

  // connect to S3 bucket
  var bucket *s3.Bucket = nil
  conn := s3.New(auth, aws.USEast)

  if conn != nil {
    bucket = conn.Bucket(bucketName)
  }

  if conn == nil || bucket == nil {
    err = errors.New("Could not connect to S3.\n")
    return nil, err
  }

  return bucket, nil
}

/* Converts the PDF in the given multipart request to a set of JPEGs. Uploads
 * the JPEGs to S3. */
func convert(writer http.ResponseWriter, request *http.Request) {
  if request.Method != "POST" {
    fmt.Fprintf(writer, "Only POST requests are supported.\n")
    return
  }

  bucket, err := connectToS3("scoryst-demo")
  if handleError(err, writer) { return }

  pdfPath, err := fetchPDF(request, bucket)
  if handleError(err, writer) { return }

  pdf, err := os.Open(pdfPath)
  if handleError(err, writer) { return }

  err = convertPDFToJPEGs(pdf.Name(), "/tmp/pages/page%d.jpg")
  if handleError(err, writer) { return }

  fmt.Printf("Conversion finished\n")
  fmt.Fprintf(writer, "Done\n")
}

/* Starts up a server to handle PDF to JPEG conversions. */
func main() {
  socket := "0.0.0.0:8000"
  fmt.Printf("Serving on %s\n", socket)

  http.HandleFunc("/", convert)
  http.ListenAndServe(socket, nil)
}
