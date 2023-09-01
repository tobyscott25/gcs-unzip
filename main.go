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
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)

	rc, err := obj.NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return err
	}

	r := bytes.NewReader(buf.Bytes())
	zipReader, err := zip.NewReader(r, int64(len(buf.Bytes())))
	if err != nil {
		return err
	}

	for _, file := range zipReader.File {
		fileReader, err := file.Open()
		if err != nil {
			return err
		}
		defer fileReader.Close()

		outFile, err := os.Create(filepath.Join(destPath, file.Name))
		if err != nil {
			return err
		}
		defer outFile.Close()

		_, err = io.Copy(outFile, fileReader)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	bucketName := "your_bucket_name"
	objectName := "path/to/your/file.zip"
	destPath := "destination_folder"

	err = unzipGCSZip(ctx, client, bucketName, objectName, destPath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Unzip complete")
}
