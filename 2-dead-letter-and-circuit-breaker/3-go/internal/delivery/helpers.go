package delivery

import (
	"bytes"
	"fmt"
	"io"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

type httpError struct{ status int }

func (e *httpError) Error() string { return fmt.Sprintf("receiver responded %d", e.status) }
