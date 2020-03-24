package eradownload

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
)

const ansi = "[\u001B\u009B][[\\]()#;?]*(?:(?:(?:[a-zA-Z\\d]*(?:;[a-zA-Z\\d]*)*)?\u0007)|(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PRZcf-ntqry=><~]))"

var re = regexp.MustCompile(ansi)

func Strip(str string) string {
	return re.ReplaceAllString(str, "")
}

func Download(date string) {
	targetFile := "data/era5-" + date + ".nc"
	_, err := os.Stat(targetFile)
	if err == nil {
		fmt.Printf("[3] ✔️ Skipping, Era5 reanalisys file exists: `%s`\n", targetFile)
		return
	}

	cmd := exec.Command("python3", "eradownload/cds.py", date, targetFile)

	stdout, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(stdout)
	fmt.Println("[3] 🡒 Downloading Era5 reanalisys file: 0%")
	m := regexp.MustCompile(`\d+\%\|`)

	for {
		buff, _, err := reader.ReadLine()

		if err == io.EOF {
			break
		}

		line := Strip(string(buff))

		perc := m.FindString(line)
		//fmt.Println(perc)
		if perc != "" {
			fmt.Printf("\033[F")
			fmt.Printf("\033[K")
			fmt.Printf("[3] 🡒 Downloading Era5 reanalisys file: %s\n", perc[0:len(perc)-1])
		}
		fmt.Fprintln(os.Stderr, line)

	}

	err = cmd.Wait()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\033[F")
	fmt.Printf("\033[K")
	fmt.Printf("[3] ✔️ Downloaded Era5 reanalisys file: `%s`\n", targetFile)
}
