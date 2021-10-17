package integration

import (
	"fmt"
	"net/http"

	"github.com/go-resty/resty/v2"
)

type Client struct {
	*resty.Client
	address string
}

func NewClient(c Config) (*Client, error) {
	result := &Client{
		Client:  resty.New(),
		address: "http://" + c.Address(),
	}

	if err := result.Ping(); err != nil {
		return nil, err
	}

	return result, nil
}

func (t *Client) Ping() error {
	resp, err := t.R().Get(t.address + "/ping")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("expecting status OK, got: %s", resp.Status())
	}
	if resp.String() != "pong" {
		return fmt.Errorf("expecting pong, got: %s", resp.String())
	}

	return nil
}

func (t *Client) GetMessages() ([]string, error) {
	var result []string
	resp, err := t.R().SetResult(&result).Get(t.address + "/messages")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("expecting status OK, got: %d", resp.StatusCode())
	}

	return result, nil
}

func (t *Client) PostMessage(m Message) error {
	var result string
	resp, err := t.R().SetBody(m).SetResult(&result).Post(t.address + "/messages")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("expecting status created, got: %d", resp.StatusCode())
	}
	if result != m.Message {
		return fmt.Errorf("expecting the sent message %v, got: %v", m, resp)
	}

	return nil
}
