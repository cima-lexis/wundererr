package wundarchive

import (
	"os"
	"path"
	"testing"
)

func TestArchive(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	err = os.Chdir(path.Dir(dir))
	if err != nil {
		panic(err)
	}

	dir, err = os.Getwd()
	if err != nil {
		panic(err)
	}

	if err := PrepareArchive("20191129"); err != nil {
		panic(err)
	}
}
