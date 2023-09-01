package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"archive/zip"
	"bytes"

	"cloud.google.com/go/storage"
)

func unzipGCSZip(ctx context.Context, client *storage.Client, bucketName, objectName, destPath string) error {
	// Create a GCS bucket and object reference
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)

	// Read the object's content from Google Cloud Storage into a buffer
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return err
	}

	// Create a new bytes.Reader from the buffer
	r := bytes.NewReader(buf.Bytes())

	// Create a new zip.Reader to read the contents of the zip file
	zipReader, err := zip.NewReader(r, int64(len(buf.Bytes())))
	if err != nil {
		return err
	}

	// Iterate through the files in the zip archive and extract them
	for _, file := range zipReader.File {
		// Open the current file within the zip archive
		fileReader, err := file.Open()
		if err != nil {
			return err
		}
		defer fileReader.Close()

		// Create a new local file to write the extracted content
		outFile, err := os.Create(filepath.Join(destPath, file.Name))
		if err != nil {
			return err
		}
		defer outFile.Close()

		// Copy the content from the zip file to the local file
		_, err = io.Copy(outFile, fileReader)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	ctx := context.Background()

	// Initialize the Google Cloud Storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Set the GCS bucket name, object name (zip file), and destination folder
	bucketName := "your_bucket_name"
	objectName := "path/to/your/file.zip"
	destPath := "destination_folder"

	// Unzip the GCS zip file to the specified destination folder
	err = unzipGCSZip(ctx, client, bucketName, objectName, destPath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Unzip complete")
}
