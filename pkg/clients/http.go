package clients

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

type httpClient struct {
	client *http.Client
}

// NewHTTPClient creates a new HTTPClient
func NewHTTPClient(client *http.Client) Client {
	return &httpClient{client}
}

func (c *httpClient) Get(u string) (Response, error) {
	resp, err := c.client.Get(u)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.Errorf("%s %s", http.StatusText(resp.StatusCode), resp.Status)
	}
	return &httpClientResponse{resp}, nil
}

func (c *httpClient) Post(u string, b []byte) (Response, error) {
	resp, err := c.client.Post(u, "application/octet-stream", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return &httpClientResponse{resp}, nil
}

type httpClientResponse struct {
	resp *http.Response
}

func (h *httpClientResponse) Status() int {
	return h.resp.StatusCode
}

func (h *httpClientResponse) Bytes() ([]byte, error) {
	return ioutil.ReadAll(h.Reader())
}

func (h *httpClientResponse) Reader() io.ReadCloser {
	return h.resp.Body
}

func (h *httpClientResponse) Close() error {
	return h.resp.Body.Close()
}
