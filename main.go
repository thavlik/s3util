package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jeffail/tunny"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// e.g. "us-east-1"
var region string

var parallelism int

func usage() {
	fmt.Print("usage: s3util <input> <output>\n")
	fmt.Print("One of the paths must start with s3://\n")
	fmt.Print("Example copy to s3:\n")
	fmt.Print("    foo.txt s3://mybucket/foo.txt\n")
	fmt.Print("Example copy from s3:\n")
	fmt.Print("    s3util s3://mybucket/foo.txt foo.txt\n")
	fmt.Print("This app uses the Go AWS SDK library (github.com/aws/aws-sdk-go)\n")
	fmt.Print("Visit github.com/thavlik/s3util for the source code and Dockerfile.\n")
}

func createSession() *session.Session {
	sess := session.Must(session.NewSession())
	sess.Config.Region = aws.String(region)
	sess.Config.Credentials = credentials.NewEnvCredentials()
	sess.Config.Endpoint = aws.String("nyc3.digitaloceanspaces.com")
	return sess
}

// splitNameParts splits an s3 path into its parts
// Examples:
// s3://mybucket/mykey	=> "mybucket", "mykey", nil
// s3://mybucket/mykey	=> "mybucket", "mykey", nil
// s3://mybucket/		=> "mybucket", "", nil (trailing / is optional)
func splitNameParts(path string) (string, string, error) {
	// get the path in `bucket/key` format
	withoutProtocol := path[len("s3://"):]
	firstSlash := strings.Index(withoutProtocol, "/")
	if firstSlash == -1 {
		// Interpret everything after the protocol as
		// the bucket name, and the target is the entire
		// bucket.
		return withoutProtocol, "", nil
	}
	bucket := withoutProtocol[0:firstSlash]
	if firstSlash == len(withoutProtocol)-1 {
		// The slash was at the end of the path, which
		// means no key was specified.
		return bucket, "", nil
	}
	// The key is everything after the slash
	key := withoutProtocol[firstSlash+1:]
	return bucket, key, nil
}

func uploadSingleFile(
	uploader *s3manager.Uploader,
	bucket *string, // sent in as string pointer for effiency's sake
	key *string,
	sourcePath string,
) error {
	f, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to read source file '%s': %v", sourcePath, err)
	}
	defer f.Close()
	if _, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: bucket,
		Key:    key,
		Body:   f,
	}); err != nil {
		return fmt.Errorf("failed to upload '%s': %v", sourcePath, err)
	}
	return fmt.Errorf("failed to upload '%s': %v", sourcePath, err)
}

func upload(source string, dest string) error {
	bucketName, key, err := splitNameParts(dest)
	if err != nil {
		return fmt.Errorf("failed to parse s3 output name parts: %v", err)
	}
	bucket := aws.String(bucketName)

	uploader := s3manager.NewUploader(createSession())

	info, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("failed to stat input path '%s': %v", source, err)
	}

	sourcePath, err := filepath.Abs(source)
	if err != nil {
		return fmt.Errorf("failed to get absolute path of source: %v", err)
	}

	keyPrefix := ""

	type uploadJob struct {
		inputFullPath string
		outputKey     string
		done          chan error
	}
	var jobs []uploadJob

	if info.IsDir() {
		sourcePathLen := len(sourcePath)

		// Specifying a target of s3://mybucket/myprefix and an input
		// path that is a folder will result in some input file `foo.txt`
		// being uploaded to s3://mybucket/myprefix/foo.txt
		// Example (upload current directory, prefix all keys with "images")
		//
		//     s3util . s3://mybucket/images
		//
		// Result: s3://mybucket/images/foo.png
		//         s3://mybucket/images/bar.png
		//         s3://mybucket/images/subdirectory/baz.jpg
		//         ...
		keyPrefix = key

		if err := filepath.Walk(
			sourcePath,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				fullPath, err := filepath.Abs(path)
				if err != nil {
					return fmt.Errorf("failed to get full path of '%s': %v", info.Name(), err)
				}

				jobs = append(jobs, uploadJob{
					inputFullPath: fullPath,
					outputKey:     fullPath[sourcePathLen:],
					done:          make(chan error, 1),
				})

				return nil
			},
		); err != nil {
			return fmt.Errorf("failed to walk source directory: %v", err)
		}
	} else {
		// Input is a specific file. Output path will either
		// be just an s3 bucket - in which case we'll use
		// the file name as the key - or it will be a key
		// that we will use verbatim.
		if key == "" {
			// No key was specified. Use the file name as the key.
			key = info.Name()
		}
		jobs = []uploadJob{
			uploadJob{
				inputFullPath: sourcePath,
				outputKey:     key,
				done:          make(chan error, 1),
			},
		}
	}

	pool := tunny.NewFunc(parallelism, func(payload interface{}) interface{} {
		j := payload.(*uploadJob)
		return uploadSingleFile(
			uploader,
			bucket,
			aws.String(fmt.Sprintf("%s/%s", keyPrefix, j.outputKey)),
			j.inputFullPath)
	})

	for i := range jobs {
		go func(job *uploadJob) {
			job.done <- func() error {
				if err, ok := pool.Process(job).(error); ok && err != nil {
					return err
				}
				return nil
			}()
		}(&jobs[i])
	}

	for i := range jobs {
		if err := <-jobs[i].done; err != nil {
		}
	}

	return nil
}

func download(source string, dest string) error {
	bucket, key, err := splitNameParts(source)
	if err != nil {
		return fmt.Errorf("failed to parse source: %v", err)
	}
	sess := createSession()
	s3Client := s3.New(sess)
	sourceLen := len(source)

	info, err := os.Stat(dest)

	if err != nil {
		if os.IsNotExist(err) {
		}
		return fmt.Errorf("failed to stat destination '%s': %v", dest, err)
	}

	if info.IsDir() {
		// Output might be key name
	}

	type downloadJob struct {
		key     string
		outPath string
		done    chan error
	}

	var jobs []downloadJob

	if source[sourceLen-1] == '*' {
		// Wildcard input: download all keys with this prefix
		out, err := s3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(source[:sourceLen]),
		})
		if err != nil {
			return fmt.Errorf("failed to list s3: %v", err)
		}
		jobs = make([]downloadJob, len(out.Contents))
		for i, obj := range out.Contents {
			//obj.Key()
			jobs[i] = downloadJob{
				key:     *obj.Key,
				outPath: "",
			}
		}
	} else {
		jobs = []downloadJob{
			downloadJob{
				key:     key,
				outPath: dest,
			},
		}
	}

	pool := tunny.NewFunc(parallelism, func(payload interface{}) interface{} {
		j := payload.(*downloadJob)
		return uploadSingleFile(
			uploader,
			bucket,
			aws.String(fmt.Sprintf("%s/%s", keyPrefix, j.outputKey)),
			j.inputFullPath,
		)
	})

	for i := range jobs {
		go func(job *downloadJob) {
			job.done <- func() error {
				if err, ok := pool.Process(job).(error); ok && err != nil {
					return err
				}
				return nil
			}()
		}(&jobs[i])
	}

	for i := range jobs {
		if err := <-jobs[i].done; err != nil {
		}
	}

	return nil
}

func entry() error {
	args := flag.Args()
	if len(args) != 2 {
		usage()
		os.Exit(1)
	}

	inPath := args[0]
	outPath := args[1]

	if strings.HasPrefix(inPath, "s3://") {
		return download(inPath, outPath)
	} else if strings.HasPrefix(outPath, "s3://") {
		return upload(inPath, outPath)
	}

	return fmt.Errorf("one of the paths must be an S3 URI (see usage)")
}

func main() {
	if err := entry(); err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}
}
