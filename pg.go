package nifi

import (
	"encoding/json"
	"fmt"
)

const (
	RUNNING = "RUNNING"
	UNKNOWN = "UNKNOWN"
	STOPPED = "STOPPED"
)

func (c *Client) GetInfo(id string) (interface{}, error) {
	path := fmt.Sprintf("/process-groups/%v", id)
	response, err := c.CallAPI(Get, path, nil)
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

	response, err := c.CallAPI(Put, path, data)
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
