package wundarchive

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func PrepareArchive(date string) error {
	cacheDir := fmt.Sprintf("data/cache/%s", date)
	archiveFile := fmt.Sprintf("data/wundarchive/wund-%s.tar.gz", date)

	f, err := os.Open(archiveFile)
	if err != nil {
		return err
	}
	defer f.Close()

	gzf, err := gzip.NewReader(f)
	if err != nil {
		return err
	}

	tarReader := tar.NewReader(gzf)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		name := header.Name

		// Store filename/path for returning and using later on
		fpath := filepath.Join(cacheDir, name)

		if header.Typeflag == tar.TypeDir {
			// Make Folder
			err = os.MkdirAll(fpath, os.ModePerm)
			if err != nil {
				return err
			}

			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0644))
		if err != nil {
			return err
		}

		_, err = io.Copy(outFile, tarReader)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()

		if err != nil {
			return err
		}
	}

	return nil
}
