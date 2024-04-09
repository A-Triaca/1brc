package file_reader

import (
	"os"
)

type Reader struct {
	Location string
}

func (input *Reader) Read() (str string, err error) {
	bs, err := os.ReadFile(input.Location)
	if err != nil {
		return "", err
	}
	content := string(bs)
	return content, nil
}
