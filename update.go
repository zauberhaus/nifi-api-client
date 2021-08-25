package nifi

import (
	"encoding/json"
	"fmt"
	"net/url"
)

type UpdateRequest struct {
	client *Client
	url    *url.URL
}

func NewUpdateRequest(client *Client, url *url.URL) *UpdateRequest {
	return &UpdateRequest{
		client: client,
		url:    url,
	}
}

func (u *UpdateRequest) Wait() (map[string]interface{}, error) {
	var result map[string]interface{}

	for {
		response, err := u.client.Call(Get, u.url, nil)
		if err != nil {
			return nil, err
		}

		request, err := u.getRequest(response)
		if err != nil {
			return nil, err
		}

		complete, ok := request["complete"].(bool)
		if !ok || complete {
			result = request
			break
		}

	}

	return result, nil
}

func (u *UpdateRequest) Close() error {
	if u.url != nil {
		_, err := u.client.Call(Delete, u.url, nil)
		return err
	}

	return nil
}

func (r *UpdateRequest) getRequest(result string) (map[string]interface{}, error) {
	var output map[string]interface{}
	err := json.Unmarshal([]byte(result), &output)
	if err != nil {
		return nil, err
	}

	request, ok := output["request"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("update request not found")
	}

	return request, nil
}
