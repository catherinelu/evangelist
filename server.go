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
  "path/filepath"
  "launchpad.net/goamz/aws"
  "launchpad.net/goamz/s3"
)

// allow at most 1 MB of form data to be passed to the server
const MAX_MULTIPART_FORM_BYTES = 1024 * 1024;

// number of workers to run simultaneously to convert a PDF
const NUM_WORKERS_CONVERT = 2;

// number of workers to run simultaneously to upload a PDF
const NUM_WORKERS_UPLOAD = 10;

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

/* See the documentation for `uploadAllJPEGsToS3`. This function does the
 * same, except for a single page. */
func uploadJPEGToS3(bucket *s3.Bucket, jpegPath string, s3JPEGPath string,
    pageNum int) error {
  jpegFile, err := os.Open(fmt.Sprintf(jpegPath, pageNum))
  if err != nil { return err }

  jpegFileInfo, err := jpegFile.Stat()
  if err != nil { return err }

  remoteJPEGPath := fmt.Sprintf(s3JPEGPath, pageNum)
  err = bucket.PutReader(remoteJPEGPath, jpegFile, jpegFileInfo.Size(),
    "image/jpeg", s3.PublicRead)
  if err != nil { return err }

  return nil
}

/* See the documentation for `uploadAllJPEGsToS3`. This function does the
 * same, except for a limited range of pages. */
func uploadJPEGRangeToS3(wg *sync.WaitGroup, bucket *s3.Bucket,
    jpegPath string, smallJPEGPath string, largeJPEGPath string,
    s3JPEGPath string, s3SmallJPEGPath string, s3LargeJPEGPath string,
    firstPage int, lastPage int) error {
  defer wg.Done()

  // upload JPEGs (normal, and large) corresponding to each page to S3
  for pageNum := firstPage; pageNum <= lastPage; pageNum = pageNum + 1 {
    err := uploadJPEGToS3(bucket, jpegPath, s3JPEGPath, pageNum)
    if err != nil { return err }

    err = uploadJPEGToS3(bucket, smallJPEGPath, s3SmallJPEGPath, pageNum)
    if err != nil { return err }

    err = uploadJPEGToS3(bucket, largeJPEGPath, s3LargeJPEGPath, pageNum)
    if err != nil { return err }
  }

  return nil
}

/* Uploads the JPEGs at the specified `jpegPath` and `largeJPEGPath` to S3. The
 * S3 name will be derived from the `s3JPEGPath` and `s3LargeJPEGPath` arguments
 * passed in the provided request. Note that all four paths mentioned above
 * should have '%d' in them. This will be replaced with the page number to get
 * the corresponding page's JPEG. */
func uploadAllJPEGsToS3(bucket *s3.Bucket, request *http.Request,
    jpegPath string, smallJPEGPath string, largeJPEGPath string,
    numPages int) error {
  s3JPEGPathSet, okJPEGPath := request.Form["s3JPEGPath"]
  s3SmallJPEGPathSet, okSmallJPEGPath := request.Form["s3SmallJPEGPath"]
  s3LargeJPEGPathSet, okLargeJPEGPath := request.Form["s3LargeJPEGPath"]

  // ensure user gives us precisely one normal JPEG and one large JPEG path
  if !okJPEGPath {
    err := errors.New("Must specify a JPEG path in the 's3JPEGPath' key.\n")
    return err
  }

  if !okSmallJPEGPath {
    err := errors.New("Must specify a small JPEG path in the " +
      "'s3SmallJPEGPath' key.\n")
    return err
  }

  if !okLargeJPEGPath {
    err := errors.New("Must specify a large JPEG path in the " +
      "'s3LargeJPEGPath' key.\n")
    return err
  }

  if len(s3JPEGPathSet) != 1 {
    err := errors.New("Must specify exactly one JPEG path in the " +
      "'s3JPEGPath' key.\n")
    return err
  }

  if len(s3SmallJPEGPathSet) != 1 {
    err := errors.New("Must specify exactly one JPEG path in the " +
      "'s3SmallJPEGPath' key.\n")
    return err
  }

  if len(s3LargeJPEGPathSet) != 1 {
    err := errors.New("Must specify exactly one JPEG path in the " +
      "'s3LargeJPEGPath' key.\n")
    return err
  }

  s3JPEGPath := request.Form["s3JPEGPath"][0]
  if !strings.Contains(s3JPEGPath, "%d") {
    err := errors.New("Must specify a JPEG path with %d in the " +
      "'s3JPEGPath' key.\n")
    return err
  }

  s3SmallJPEGPath := request.Form["s3SmallJPEGPath"][0]
  if !strings.Contains(s3SmallJPEGPath, "%d") {
    err := errors.New("Must specify a JPEG path with %d in the " +
      "'s3SmallJPEGPath' key.\n")
    return err
  }

  s3LargeJPEGPath := request.Form["s3LargeJPEGPath"][0]
  if !strings.Contains(s3LargeJPEGPath, "%d") {
    err := errors.New("Must specify a JPEG path with %d in the " +
      "'s3LargeJPEGPath' key.\n")
    return err
  }

  // find number of pages to upload per worker
  numPagesPerWorkerFloat64 := float64(numPages) / float64(NUM_WORKERS_UPLOAD)
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

    go uploadJPEGRangeToS3(&wg, bucket, jpegPath, smallJPEGPath, largeJPEGPath,
      s3JPEGPath, s3SmallJPEGPath, s3LargeJPEGPath, firstPage, lastPage)
  }

  wg.Wait()
  return nil
}

/* Resizes the JPEG at `jpegPath` to have a width at most `maxWidth` and
 * a height at most `maxHeight`. Maintains aspect ratio. Saves the resized
 * JPEG to `resizedJPEGPath`. */
func resizeAndSaveImage(jpegPath string, resizedJPEGPath string, maxWidth int,
    maxHeight int) error {
  dimension := fmt.Sprintf("%dx%d", maxWidth, maxHeight)
  cmd := exec.Command("convert", "-resize", dimension, jpegPath, resizedJPEGPath)
  return cmd.Run()
}

/* Converts the PDF at `pdfPath` to JPEGs. Outputs the JPEGs to the provided
 * `jpegPath` (note: '%d' in `jpegPath` will be replaced by the JPEG
 * number). Converts pages within the range [`firstPage`, `lastPage`]. Calls
 * `wg.Done()` once finished. Returns an error on the given channel. */
func convertPagesToJPEGs(wg *sync.WaitGroup, pdfPath string, jpegPath string,
    smallJPEGPath string, largeJPEGPath string, firstPage int, lastPage int) {
  defer wg.Done()

  // use ghostscript for PDF -> JPEG conversion at 300 density
  for pageNum := firstPage; pageNum <= lastPage; pageNum = pageNum + 1 {
    // convert a single page at a time with the correct output JPEG path
    firstPageOption := fmt.Sprintf("-dFirstPage=%d", pageNum)
    lastPageOption := fmt.Sprintf("-dLastPage=%d", pageNum)

    // convert to two sizes: normal and large
    jpegPathForPage := fmt.Sprintf(jpegPath, pageNum)
    smallJPEGPathForPage := fmt.Sprintf(smallJPEGPath, pageNum)
    largeJPEGPathForPage := fmt.Sprintf(largeJPEGPath, pageNum)

    outputFileOption := fmt.Sprintf("-sOutputFile=%s", largeJPEGPathForPage)

    cmd := exec.Command("gs", "-dNOPAUSE", "-sDEVICE=jpeg", firstPageOption,
      lastPageOption, outputFileOption, "-dJPEGQ=90", "-r200", "-q", pdfPath,
      "-c", "quit")
    err := cmd.Run()

    if err != nil {
      fmt.Printf("gs command failed: %s\n", err.Error())
      return
    }

    resizeAndSaveImage(largeJPEGPathForPage, jpegPathForPage, 800, 800)
    if err != nil {
      fmt.Printf("Couldn't resize image: %s\n", err.Error())
      return
    }

    resizeAndSaveImage(jpegPathForPage, smallJPEGPathForPage, 300, 300)
    if err != nil {
      fmt.Printf("Couldn't resize image: %s\n", err.Error())
      return
    }
  }
}

/* Converts the PDF at `pdfPath` to JPEGs. Outputs the JPEGs to the provided
 * `jpegPath` (note: '%d' in `jpegPath` will be replaced by the JPEG
 * number). Returns the path to the JPEGs (contains a %d that should be
 * replaced with the page number) and the number of pages in the PDF. */
func convertPDFToJPEGs(pdfPath string, jpegPath string, smallJPEGPath string,
    largeJPEGPath string) (int, error) {
  numPages, err := getNumPages(pdfPath)
  if err != nil { return -1, err }

  // find number of pages to convert per worker
  numPagesPerWorkerFloat64 := float64(numPages) / float64(NUM_WORKERS_CONVERT)
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

    go convertPagesToJPEGs(&wg, pdfPath, jpegPath, smallJPEGPath,
      largeJPEGPath, firstPage, lastPage)
  }

  wg.Wait()
  return numPages, err
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

/* Finds the PDF the user would like to convert. Downloads it to a temporary
 * file for processing. Returns the temporary file path. */
func fetchPDF(request *http.Request, bucket *s3.Bucket) (string, error) {
  err := request.ParseMultipartForm(MAX_MULTIPART_FORM_BYTES)
  if err != nil { return "", err }

  s3PDFPathSet, ok := request.Form["s3PDFPath"]

  // ensure user gives us precisely one PDF to convert
  if !ok {
    err = errors.New("Must specify a PDF to convert in the 's3PDFPath' key.\n")
    return "", err
  }

  if len(s3PDFPathSet) != 1 {
    err = errors.New("Must specify exactly one S3 PDF path in 's3PDFPath' key.\n")
    return "", err
  }

  // find PDF in S3
  s3PDFPath := request.Form["s3PDFPath"][0]
  reader, err := bucket.GetReader(s3PDFPath)

  if err != nil { return "", err }
  defer reader.Close()

  // copy multipart data into temporary file for processing
  pdfPath := "/tmp/" + generateRandomString(50) + ".pdf"
  pdf, err := os.Create(pdfPath)

  if err != nil { return "", err }
  defer pdf.Close()

  _, err = io.Copy(pdf, reader)
  if err != nil { return "", err }

  return pdfPath, nil
}

/* Returns an S3 connection to the given bucket. */
func connectToS3(bucketName string, region aws.Region) (*s3.Bucket, error) {
  auth, err := aws.EnvAuth()
  if err != nil { return nil, err }

  // connect to S3 bucket
  var bucket *s3.Bucket = nil
  conn := s3.New(auth, region)

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
func convert(writer http.ResponseWriter, request *http.Request,
    bucketName string, regionName string) {
  if request.Method != "POST" {
    fmt.Fprintf(writer, "Only POST requests are supported.\n")
    return
  }

  bucket, err := connectToS3(bucketName, aws.Regions[regionName])
  if handleError(err, writer) { return }

  pdfPath, err := fetchPDF(request, bucket)
  if handleError(err, writer) { return }

  // put JPEGs in tmp folder under random prefix
  jpegPrefix := generateRandomString(50);
  jpegPath := fmt.Sprintf("/tmp/%s%%d.jpg", jpegPrefix);
  smallJPEGPath := fmt.Sprintf("/tmp/%s%%d-small.jpg", jpegPrefix);
  largeJPEGPath := fmt.Sprintf("/tmp/%s%%d-large.jpg", jpegPrefix);

  numPages, err := convertPDFToJPEGs(pdfPath, jpegPath, smallJPEGPath,
    largeJPEGPath)
  if handleError(err, writer) { return }

  err = uploadAllJPEGsToS3(bucket, request, jpegPath, smallJPEGPath,
    largeJPEGPath, numPages)
  if handleError(err, writer) { return }

  fmt.Printf("Conversion finished\n")
  fmt.Fprintf(writer, "Done\n")
}

/* Starts up a server to handle PDF to JPEG conversions. */
func main() {
  socket := "0.0.0.0:7000"
  fmt.Printf("Serving on %s\n", socket)

  // must have three arguments: file path, bucket name, and region name
  if len(os.Args) != 3 {
    baseName := filepath.Base(os.Args[0])
    fmt.Printf("Usage: %s [bucketName] [regionName]\n", baseName)
    os.Exit(1)
  }

  bucketName := os.Args[1]
  regionName := os.Args[2]

  http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
    convert(writer, request, bucketName, regionName)
  })
  http.ListenAndServe(socket, nil)
}
