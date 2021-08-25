package nifi

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	ErrInvalidFormat = fmt.Errorf("invalid response fornat")
)

type Method string

const (
	Get     Method = "GET"
	Post    Method = "POST"
	Delete  Method = "DELETE"
	Put     Method = "PUT"
	Unknown Method = ""
)

const (
	RUNNING = "RUNNING"
	UNKNOWN = "UNKNOWN"
	STOPPED = "STOPPED"
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
	client, err := NewLoginClient(certFile, password, insecureSkipVerify)
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
				log.Debug("Use existing token valid until %v\n", status.Expire)
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
	return c.call(Get, path, nil, query...)
}

func (c *Client) Post(path string, data []byte, query ...string) (string, error) {
	return c.call(Post, path, data, query...)
}

func (c *Client) Put(path string, data []byte, query ...string) (string, error) {
	return c.call(Put, path, data, query...)
}

func (c *Client) Delete(path string, query ...string) (string, error) {
	return c.call(Delete, path, nil, query...)
}

func (c *Client) call(method Method, path string, data []byte, query ...string) (string, error) {
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

	data, err := c.call(Get, "/process-groups/root", nil)
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

func (c *Client) Tree(ids []string, types NiFiType) (Tree, error) {
	tree := Tree{}

	for _, id := range ids {
		t, err := c.tree(id, types, true, nil)
		if err != nil {
			return nil, err
		}

		tree.Merge(t)

	}

	return tree, nil
}

func (c *Client) All(ids []string, types NiFiType, recursive bool) ([]*Component, error) {
	result := []*Component{}

	for _, id := range ids {
		list, err := c.all(id, types, recursive, nil)
		if err != nil {
			return nil, err
		}

		result = append(result, list...)

	}

	return result, nil
}

func (c *Client) AllWith(ids []string, types NiFiType, recursive bool, filter ComponentFilter) ([]*Component, error) {
	result := []*Component{}

	for _, id := range ids {
		list, err := c.all(id, types, recursive, filter)
		if err != nil {
			return nil, err
		}

		result = append(result, list...)

	}

	return result, nil
}

func (c *Client) tree(id string, types NiFiType, recursive bool, filter ComponentFilter) (Tree, error) {
	data, err := c.Get(fmt.Sprintf("/flow/process-groups/%v/status", id),
		"recursive="+strconv.FormatBool(recursive),
	)

	if err != nil {
		return nil, err
	}

	var input map[string]interface{}
	err = json.Unmarshal([]byte(data), &input)
	if err != nil {
		return nil, err
	}

	val, ok := input["processGroupStatus"]
	if !ok {
		return nil, ErrInvalidFormat
	}

	input, ok = val.(map[string]interface{})
	if !ok {
		return nil, ErrInvalidFormat
	}

	val, ok = input["aggregateSnapshot"]
	if !ok {
		return nil, ErrInvalidFormat
	}

	input, ok = val.(map[string]interface{})
	if !ok {
		return nil, ErrInvalidFormat
	}

	tree := Tree{}
	err = c.loopTree("processGroupStatusSnapshots", tree, input, types, filter)
	if err != nil {
		return nil, err
	}

	return tree, nil
}

func (c *Client) all(id string, types NiFiType, recursive bool, filter ComponentFilter) ([]*Component, error) {
	data, err := c.Get(fmt.Sprintf("/flow/process-groups/%v/status", id),
		"recursive="+strconv.FormatBool(recursive),
	)

	if err != nil {
		return nil, err
	}

	var input map[string]interface{}
	err = json.Unmarshal([]byte(data), &input)
	if err != nil {
		return nil, err
	}

	val, ok := input["processGroupStatus"]
	if !ok {
		return nil, ErrInvalidFormat
	}

	input, ok = val.(map[string]interface{})
	if !ok {
		return nil, ErrInvalidFormat
	}

	val, ok = input["aggregateSnapshot"]
	if !ok {
		return nil, ErrInvalidFormat
	}

	input, ok = val.(map[string]interface{})
	if !ok {
		return nil, ErrInvalidFormat
	}

	list, err := c.loop(0, "processGroupStatusSnapshots", "", input, types, recursive, filter)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (c *Client) loop(level int, name string, path string, o map[string]interface{}, types NiFiType, recursive bool, filter ComponentFilter) ([]*Component, error) {
	result := []*Component{}

	if len(o) == 0 {
		return result, nil
	}

	name = strings.Replace(name, "StatusSnapshots", "", -1)
	name = strings.ToLower(name)

	component := NewComponent(name, path, o)
	if component != nil {
		if len(path) == 0 && component.ID != c.root.ID {
			path += "."
		}

		path = path + "/" + component.Name
		if (component.Type & types) > 0 {
			if filter == nil || filter(component) {
				result = append(result, component)
			}
		}
	} else {
		path = path + "/?"
	}

	if recursive || level == 0 {
		for k, v := range o {
			if !recursive && k == "processGroupStatusSnapshots" {
				continue
			}

			if strings.HasSuffix(k, "StatusSnapshots") {
				input, ok := v.([]interface{})
				if !ok {
					return nil, ErrInvalidFormat
				}

				if len(input) > 0 {
					for _, s := range input {

						snapshot, err := c.getSnapshot(s)
						if err != nil {
							return nil, err
						}

						l, err := c.loop(level+1, k, path, snapshot, types, recursive, filter)
						if err != nil {
							return nil, err
						}

						result = append(result, l...)
					}
				}
			}

		}
	}

	sort.Sort(ByType(result))

	return result, nil
}

func (c *Client) loopTree(name string, parent Tree, o map[string]interface{}, types NiFiType, filter ComponentFilter) error {
	tree := Tree{}

	if len(o) == 0 {
		return nil
	}

	name = strings.Replace(name, "StatusSnapshots", "", -1)
	name = strings.ToLower(name)

	component := NewComponent(name, "", o)
	if component != nil {
		if (component.Type & types) > 0 {
			if filter == nil || filter(component) {
				parent[component] = tree
			}
		}
	}

	for k, v := range o {
		if strings.HasSuffix(k, "StatusSnapshots") {
			input, ok := v.([]interface{})
			if !ok {
				return ErrInvalidFormat
			}

			if len(input) > 0 {
				for _, s := range input {

					snapshot, err := c.getSnapshot(s)
					if err != nil {
						return err
					}

					err = c.loopTree(k, tree, snapshot, types, filter)
					if err != nil {
						return err
					}
				}
			}
		}

	}

	return nil
}

func (c *Client) getSnapshot(o interface{}) (map[string]interface{}, error) {
	input, ok := o.(map[string]interface{})
	if !ok {
		return nil, ErrInvalidFormat
	}

	for k, v := range input {
		if strings.HasSuffix(k, "StatusSnapshot") {
			input, ok := v.(map[string]interface{})
			if !ok {
				return nil, ErrInvalidFormat
			}

			return input, nil
		}
	}

	return nil, nil
}

func (c *Client) GetInfo(id string) (interface{}, error) {
	path := fmt.Sprintf("/process-groups/%v", id)
	response, err := c.call(Get, path, nil)
	if err != nil {
		return nil, err
	}

	var output interface{}
	err = json.Unmarshal([]byte(response), &output)
	if err != nil {
		return "", err
	}

	return output, nil
}

func (c *Client) SetState(id string, state string) (string, error) {

	body := &RunningStatus{
		Id:                           id,
		State:                        state,
		DisconnectedNodeAcknowledged: false,
	}

	path := fmt.Sprintf("/flow/process-groups/%v", id)

	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	response, err := c.call(Put, path, data)
	if err != nil {
		return "", err
	}

	var output interface{}
	err = json.Unmarshal([]byte(response), &output)
	if err != nil {
		return "", err
	}

	result, ok := output.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("unexpected response format")
	}

	state, ok = result["state"].(string)
	if !ok {
		return "", fmt.Errorf("result state not found")
	}

	return state, nil
}
