# Evangelist
Evangelist converts PDF to JPEGs quickly using [Golang](http://golang.org/),
[Ghostscript](http://www.ghostscript.com/), and
[epeg](https://github.com/mattes/epeg).

To use it, first run the server:

```bash
$ go run server.go
# => Serving on 0.0.0.0:8000
```

Then, make a POST request to the server's root path with two parameters:

1. `s3PDFPath`: The S3 path that refers to the PDF to convert.
2. `s3JPEGPath`: The S3 path to upload JPEGs to. This should include a %d
   symbol that will be replaced by the page number.

Three JPEGs will be uploaded for each page: one small, one normal, and one
large. The %d in `s3JPEGPath` will actually be replaced by "{pageNumber}-small"
for the small JPEG, "{pageNumber}" for the normal JPEG and "{pageNumber}-large"
  for the large JPEG.

For instance, if `s3JPEGPath` is "split-pages/page%d.jpg", then:

- A small JPEG for page 1 will be uploaded to "split-pages/page1-small.jpg"
- A normal JPEG for page 1 will be uploaded to "split-pages/page1.jpg"
- A large JPEG for page 1 will be uploaded to "split-pages/page1-large.jpg"

Here's an example request:

```bash
$ curl -d
"s3PDFPath=exam-pdf/0mzvCOQXkGixnu0LrUGjPuagFevCQ120140203000038.pdf&s3JPEGPath=split-pages/gotest%25d.jpg"
localhost:8000
# => Done
```

Note that the '%' sign in `s3JPEGPath` must be escaped as '%25' due to the
rules of parsing multipart form content.

The above command downloads the given PDF, converts all 6 pages into 18 JPEGs,
and uploads the JPEGs to S3 in ~7.5 seconds.
