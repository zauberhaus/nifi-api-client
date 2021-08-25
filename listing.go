package nifi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/zauberhaus/nifi-api-client/filter"
)

type FlowFile struct {
	Uuid            string    `json:"uuid"`
	Filename        string    `json:"filename"`
	Size            int       `json:"size"`
	LineageDuration float64   `json:"lineageDuration"`
	QueuedDuration  float64   `json:"queuedDuration"`
	LinageStart     time.Time `json:"linageStart"`
	QueuedStart     time.Time `json:"queuedStart"`
	Penalized       bool      `json:"penalized"`
	Node            string    `json:"node"`
}

type ListingRequest struct {
	client Client

	connection string

	id  string
	url *url.URL
}

func NewListingRequest(client Client, connection string) (*ListingRequest, error) {
	result := &ListingRequest{
		client:     client,
		connection: connection,
	}

	body, err := client.Post("/flowfile-queues/"+connection+"/listing-requests", nil)
	if err != nil {
		return nil, err
	}

	request, err := result.getRequest(body)
	if err != nil {
		return nil, err
	}

	id, ok := request["id"].(string)
	if !ok {
		return nil, fmt.Errorf("listing-request: id not found")
	}

	uri, ok := request["uri"].(string)
	if !ok {
		return nil, fmt.Errorf("listing-request: uri not found")
	}

	url, err := url.Parse(uri)
	if !ok {
		return nil, err
	}

	result.id = id
	result.url = url

	return result, nil
}

func (r *ListingRequest) List() ([]FlowFile, error) {
	result, err := r.client.Call(Get, r.url, nil)
	if err != nil {
		return nil, err
	}

	request, err := r.getRequest(result)
	if err != nil {
		return nil, err
	}

	flowFiles := []FlowFile{}

	for {

		now := time.Now()

		finished, ok := request["finished"].(bool)
		if !ok {
			return nil, fmt.Errorf("listing-request: finished value not found")
		}

		filter.Map(request, ".flowFileSummaries[]", func(val interface{}) (bool, error) {
			file, ok := val.(map[string]interface{})
			if !ok {
				return true, nil
			}

			uuid, ok := file["uuid"].(string)
			if !ok {
				return true, nil
			}

			filename, ok := file["filename"].(string)
			if !ok {
				filename = ""
			}

			node, ok := file["clusterNodeAddress"].(string)
			if !ok {
				return true, nil
			}

			size, ok := file["size"].(float64)
			if !ok {
				return true, nil
			}

			lineageDuration, ok := file["lineageDuration"].(float64)
			if !ok {
				return true, nil
			}

			queuedDuration, ok := file["queuedDuration"].(float64)
			if !ok {
				return true, nil
			}

			item := FlowFile{
				Uuid:            uuid,
				Filename:        filename,
				Node:            node,
				Size:            int(size),
				LinageStart:     now.Add(-time.Duration(lineageDuration) * time.Millisecond),
				QueuedStart:     now.Add(-time.Duration(queuedDuration) * time.Millisecond),
				LineageDuration: lineageDuration / 1000,
				QueuedDuration:  queuedDuration / 1000,
			}

			flowFiles = append(flowFiles, item)

			return true, nil
		})

		if finished {
			break
		}
	}

	return flowFiles, nil
}

func (r *ListingRequest) Close() error {
	if r != nil && len(r.id) > 0 {

		result, err := r.client.Delete("/flowfile-queues/" + r.connection + "/listing-requests/" + r.id)
		if err != nil {
			return err
		}

		request, err := r.getRequest(result)
		if err != nil {
			return err
		}

		finished, ok := request["finished"].(bool)
		if !ok {
			return fmt.Errorf("listing-request: finished value not found")
		}

		if !finished {
			return fmt.Errorf("listing-request: finished failed")
		}
	}

	return nil
}

func (r *ListingRequest) getRequest(result string) (map[string]interface{}, error) {
	var output map[string]interface{}
	err := json.Unmarshal([]byte(result), &output)
	if err != nil {
		return nil, err
	}

	request, ok := output["listingRequest"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("listing-request not found")
	}

	return request, nil
}
