package wundarchive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func PrepareArchive(date string) error {
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

	result := make(map[string]string)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		name := header.Name

		if header.Typeflag == tar.TypeDir {
			// Make Folder
			//err = os.MkdirAll(fpath, os.ModePerm)
			//if err != nil {
			//	return err
			//}

			continue
		}

		baseName := filepath.Base(name)
		ext := filepath.Ext(baseName)
		stationID := baseName[0 : len(baseName)-len(ext)]

		buf := new(bytes.Buffer)

		_, err = buf.ReadFrom(tarReader)
		if err != nil {
			return err
		}

		stationNewData := buf.String()
		stationNewData = strings.ReplaceAll(stationNewData, "\n", "")

		stationData, ok := result[stationID]
		if !ok {
			stationData = "{\"observations\": [" + stationNewData
			result[stationID] = stationData
		} else {
			result[stationID] = stationData + "," + stationNewData
		}

		/*
			// Make File
			if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
				return err
			}

			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0644))
			if err != nil {
				return err
			}
		*/

		//_, err = io.Copy(outFile, tarReader)
		//if err != nil {
		//	return err
		//}
		// Close the file without defer to close before next iteration of loop
		//outFile.Close()
		fmt.Fprintln(os.Stderr, stationID)
	}

	cacheDir := fmt.Sprintf("data/cache/%s", date)

	// Make File
	if err = os.MkdirAll(cacheDir, os.ModePerm); err != nil {
		return err
	}

	for stationID, data := range result {
		cacheFile := fmt.Sprintf("data/cache/%s/%s.json", date, stationID)
		data += "]}"

		outFile, err := os.OpenFile(cacheFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0644))
		if err != nil {
			return err
		}

		_, err = outFile.WriteString(data)
		if err != nil {
			return err
		}

		outFile.Close()
	}

	return nil
}
