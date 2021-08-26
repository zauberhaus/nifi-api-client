package nifi

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	ErrInvalidFormat = fmt.Errorf("invalid response fornat")
)

type Method string

const (
	Get     Method = http.MethodGet
	Post    Method = http.MethodPost
	Delete  Method = http.MethodDelete
	Put     Method = http.MethodPut
	Unknown Method = ""
)

type RunningStatus struct {
	Id                           string `json:"id"`
	State                        string `json:"state"`
	DisconnectedNodeAcknowledged bool   `json:"disconnectedNodeAcknowledged"`
}

type Client struct {
	client *HttpClient
	server *url.URL
	status *Status

	root *Component
}

type ComponentFilter func(*Component) bool

func Connect(server *url.URL, certFile string, password string, insecureSkipVerify bool) (*Client, error) {
	client, err := NewHttpCertClient(certFile, password, insecureSkipVerify)
	if err != nil {
		return nil, err
	}

	rc := &Client{
		client: client,
		server: server,
		status: &Status{
			Server:   server.String(),
			Insecure: insecureSkipVerify,
		},
	}

	root, err := rc.Root()
	if err != nil {
		return nil, err
	}

	rc.root = root

	return rc, nil
}

func Login(server *url.URL, username string, password string, options ...interface{}) (*Client, error) {
	skip := false
	if len(options) > 0 {
		val, ok := options[0].(bool)
		if ok {
			skip = val
		}
	}

	status := &Status{}
	err := status.Load("./token.yaml")
	if err == nil && username == status.User &&
		(server == nil || server.String() == status.Server) &&
		status.Expire.After(time.Now()) {

		client, err := NewHttpClient(status.Insecure)
		if err != nil {
			return nil, err
		}

		url, err := url.ParseRequestURI(status.Server)
		if err == nil {
			c := &Client{
				client: client,
				server: url,
				status: status,
			}

			root, err := c.Root()
			if err == nil {
				c.root = root
				return c, nil
			}

		}
	}

	client, err := NewHttpClient(skip)
	if err != nil {
		return nil, err
	}

	data := url.Values{}
	data.Set("username", username)
	data.Set("password", password)

	u := *server
	u.Path = "/nifi-api/access/token"

	request, err := http.NewRequest("POST", u.String(), strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	token, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode > 299 {
		return nil, fmt.Errorf("login: %v (%v)", string(token), response.Status)
	}

	status, err = NewStatus(server, string(token), response.Cookies(), skip)
	if err != nil {
		return nil, err
	}

	status.Save("./token.yaml")

	rc := &Client{
		client: client,
		server: server,
		status: status,
	}

	root, err := rc.Root()
	if err != nil {
		return nil, err
	}

	rc.root = root

	return rc, nil
}

func (c *Client) Cluster() (string, error) {
	return c.Get("/controller/cluster")
}

func (c *Client) Get(path string, query ...string) (string, error) {
	return c.CallAPI(Get, path, nil, query...)
}

func (c *Client) Post(path string, data []byte, query ...string) (string, error) {
	return c.CallAPI(Post, path, data, query...)
}

func (c *Client) Put(path string, data []byte, query ...string) (string, error) {
	return c.CallAPI(Put, path, data, query...)
}

func (c *Client) Delete(path string, query ...string) (string, error) {
	return c.CallAPI(Delete, path, nil, query...)
}

func (c *Client) CallAPI(method Method, path string, data []byte, query ...string) (string, error) {
	u := *c.server
	u.Path = "/nifi-api" + path

	if len(query) > 0 {
		u.RawQuery = url.PathEscape(strings.Join(query, "&"))
	}

	return c.Call(method, &u, data)
}

func (c *Client) Call(method Method, url *url.URL, data []byte) (string, error) {

	var reader io.Reader = nil
	if len(data) > 0 {
		reader = strings.NewReader(string(data))
	}

	request, err := c.status.NewRequest(string(method), url.String(), reader)
	if err != nil {
		return "", err
	}

	request.Header.Add("Content-Type", "application/json")

	response, err := c.client.Do(request)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("%s: %s", response.Status, body)
	}

	contentType := response.Header.Get("Content-Type")
	if contentType != "application/json" {
		return "", fmt.Errorf("unexpected content type: %v\n%v", contentType, string(body))
	}

	return string(body), nil
}

func (c *Client) Root() (*Component, error) {
	if c.root != nil {
		return c.root, nil
	}

	data, err := c.CallAPI(Get, "/process-groups/root", nil)
	if err != nil {
		return nil, err
	}

	var input map[string]interface{}
	err = json.Unmarshal([]byte(data), &input)
	if err != nil {
		return nil, err
	}

	cp, ok := input["component"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data type")
	}

	id, ok := cp["id"].(string)
	if !ok {
		return nil, fmt.Errorf("root id not found")
	}

	name, ok := cp["name"].(string)
	if !ok {
		return nil, fmt.Errorf("root name not found")
	}

	return &Component{
		ID:         id,
		Name:       name,
		Path:       "",
		Type:       ProcessGroup,
		TypeName:   ProcessGroupTitle,
		Attributes: cp,
	}, nil
}
