package shttp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Logger interface {
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

// SeekingHTTP uses a series of HTTP GETs with Range headers
// to implement io.ReadSeeker and io.ReaderAt.
type SeekingHTTP struct {
	URL           string
	Client        HttpClient
	url           *url.URL
	offset        int64
	last          *bytes.Buffer
	lastOffset    int64
	contentLength int64
	Logger        Logger
}

// Compile-time check of interface implementations.
var _ io.ReadSeeker = (*SeekingHTTP)(nil)
var _ io.ReaderAt = (*SeekingHTTP)(nil)

// New initializes a SeekingHTTP for the given URL.
// The SeekingHTTP.Client field may be set before the first call
// to Read or Seek.
func New(url string) *SeekingHTTP {
	return &SeekingHTTP{
		URL:           url,
		offset:        0,
		contentLength: -1,
	}
}

func (s *SeekingHTTP) SetLogger(logger Logger) {
	s.Logger = logger
}

func (s *SeekingHTTP) newReq() (*http.Request, error) {
	var err error
	if s.url == nil {
		s.url, err = url.Parse(s.URL)
		if err != nil {
			return nil, err
		}
	}
	return &http.Request{
		Method:     "GET",
		URL:        s.url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       nil,
		Host:       s.url.Host,
	}, nil
}

func fmtRange(from, l int64) string {
	var to int64
	if l == 0 {
		to = from
	} else {
		to = from + (l - 1)
	}

	return fmt.Sprintf("bytes=%v-%v", from, to)
}

// ReadAt reads len(buf) bytes into buf starting at offset off.
func (s *SeekingHTTP) ReadAt(buf []byte, off int64) (n int, err error) {
	if s.Logger != nil {
		s.Logger.Debugf("ReadAt len %v off %v", len(buf), off)
	}

	if off < 0 {
		return 0, io.EOF
	}

	if s.last != nil && off > s.lastOffset {
		end := off + int64(len(buf))
		if end <= s.lastOffset+int64(s.last.Len()) {
			start := off - s.lastOffset
			if s.Logger != nil {
				s.Logger.Debugf("cache hit: range (%v-%v) is within cache (%v-%v)", off, off+int64(len(buf)), s.lastOffset, s.lastOffset+int64(s.last.Len()))
			}
			copy(buf, s.last.Bytes()[start:end-s.lastOffset])
			return len(buf), nil
		}
	}

	if s.Logger != nil {
		if s.last != nil {
			s.Logger.Debugf("cache miss: range (%v-%v) is NOT within cache (%v-%v)", off, off+int64(len(buf)), s.lastOffset, s.lastOffset+int64(s.last.Len()))
		} else {
			s.Logger.Debugf("cache miss: cache empty")
		}
	}

	req, err := s.newReq()
	if err != nil {
		return 0, err
	}

	// Minimum fetch size is 1 meg
	wanted := 1024 * 1024
	if wanted < len(buf) {
		wanted = len(buf)
	}

	rng := fmtRange(off, int64(wanted))
	req.Header.Add("Range", rng)

	if s.last == nil {
		// Cache does not exist yet. So make it.
		s.last = &bytes.Buffer{}
	} else {
		// Cache is getting replaced. Bring it back to zero bytes, but
		// keep the underlying []byte, since we'll reuse it right away.
		s.last.Reset()
	}

	if s.Logger != nil {
		s.Logger.Infof("Start HTTP GET with Range: %s", rng)
	}

	if err := s.init(); err != nil {
		return 0, err
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return 0, err
	}

	// body needs to be closed, even if responses that aren't 200 or 206
	defer func(body io.ReadCloser) {
		cErr := body.Close()
		if err == nil && cErr != nil {
			err = cErr
		}
	}(resp.Body)

	if s.Logger != nil {
		s.Logger.Infof("Response status: %v", resp.StatusCode)
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		_, err := s.last.ReadFrom(resp.Body)
		if err != nil {
			return 0, err
		}
		if s.Logger != nil {
			s.Logger.Debugf("loaded %d bytes into last", s.last.Len())
		}

		s.lastOffset = off
		var n int
		if s.last.Len() < len(buf) {
			n = s.last.Len()
			copy(buf, s.last.Bytes()[0:n])
		} else {
			n = len(buf)
			copy(buf, s.last.Bytes())
		}

		// HTTP is trying to tell us, "that's all". Which is fine, but we don't
		// want callers to think it is EOF, it's not.
		if err == io.EOF && n == len(buf) {
			err = nil
		}

		return n, err
	}
	return 0, io.EOF
}

// If they did not give us an HTTP Client, use the default one.
func (s *SeekingHTTP) init() error {
	if s.Client == nil {
		s.Client = http.DefaultClient
	}

	return nil
}

func (s *SeekingHTTP) Read(buf []byte) (int, error) {
	if s.Logger != nil {
		s.Logger.Debugf("got read len %v", len(buf))
	}

	n, err := s.ReadAt(buf, s.offset)
	if err == nil {
		s.offset += int64(n)
	}

	return n, err
}

// Seek sets the offset for the next Read.
func (s *SeekingHTTP) Seek(offset int64, whence int) (int64, error) {
	if s.Logger != nil {
		s.Logger.Debugf("got seek %v %v", offset, whence)
	}

	switch whence {
	case io.SeekStart:
		s.offset = offset
	case io.SeekCurrent:
		s.offset += offset
	case io.SeekEnd:
		sz, err := s.Size()
		if err != nil {
			return 0, err
		}
		s.offset = sz + offset
	default:
		return 0, os.ErrInvalid
	}

	return s.offset, nil
}

// Size uses an HTTP HEAD to find out how many bytes are available in total.
func (s *SeekingHTTP) Size() (int64, error) {
	if err := s.init(); err != nil {
		return 0, err
	}
	if s.contentLength >= 0 {
		return s.contentLength, nil
	}

	req, err := s.newReq()
	if err != nil {
		return 0, err
	}
	req.Method = "HEAD"

	resp, err := s.Client.Do(req)
	if err != nil {
		return 0, err
	}

	if resp.ContentLength < 0 {
		return 0, errors.New("no content length for Size()")
	}
	s.contentLength = resp.ContentLength

	if s.Logger != nil {
		s.Logger.Debugf("url: %v, size %v", req.URL.String(), resp.ContentLength)
	}
	return resp.ContentLength, nil
}

func (s *SeekingHTTP) Close() error {
	if s.Logger != nil {
		s.Logger.Debugf("close %v", s.offset)
	}
	return nil
}
