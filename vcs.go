/*
Copyright © 2021 Dirk Lembke

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nifi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/zauberhaus/nifi-api-client/filter"
)

type Revision struct {
	ClientId string `json:"clientId"`
	Version  int    `json:"version"`
}

type VersionControlInfo struct {
	GroupId  string `json:"groupId" yaml:"groupId"`
	Registry string `json:"registryId"`
	Bucket   string `json:"bucketId"`
	Flow     string `json:"flowId"`
	Version  int    `json:"version"`
	State    string `json:"state"`
}

type ProcessGroupVersion struct {
	Version   float64   `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Comments  string    `json:"comments"`
}

type VersionInfo struct {
	Revision                     *Revision           `json:"processGroupRevision"`
	Version                      *VersionControlInfo `json:"versionControlInformation"`
	DisconnectedNodeAcknowledged bool                `json:"disconnectedNodeAcknowledged"`
}

type ByVersion []ProcessGroupVersion

func (a ByVersion) Len() int { return len(a) }
func (a ByVersion) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByVersion) Less(i, j int) bool {
	return a[i].Version > a[j].Version
}

func (c *Client) GetVersionControlInfo(id string) (*VersionControlInfo, *Revision, error) {
	url := fmt.Sprintf("/versions/process-groups/%v", id)

	response, err := c.CallAPI(Get, url, nil)
	if err != nil {
		return nil, nil, err
	}

	var output interface{}
	err = json.Unmarshal([]byte(response), &output)
	if err != nil {
		return nil, nil, err
	}

	vci, err := NewVersionControlInfo(output)
	if err != nil {
		return nil, nil, err
	}

	revision, err := NewRevision(output)
	if err != nil {
		return nil, nil, err
	}

	return vci, revision, nil
}

func (c *Client) GetVersions(registry string, bucket string, flow string) ([]ProcessGroupVersion, error) {
	url := fmt.Sprintf("/flow/registries/%v/buckets/%v/flows/%v/versions", registry, bucket, flow)

	response, err := c.CallAPI(Get, url, nil)
	if err != nil {
		return nil, err
	}

	var output interface{}
	err = json.Unmarshal([]byte(response), &output)
	if err != nil {
		return nil, err
	}

	list := []ProcessGroupVersion{}

	filter.Map(output, ".versionedFlowSnapshotMetadataSet[] | .versionedFlowSnapshotMetadata | { version: .version, timestamp: .timestamp, comments: .comments}", func(val interface{}) (bool, error) {
		obj := val.(map[string]interface{})

		t, ok := obj["timestamp"].(float64)
		if !ok {
			return false, fmt.Errorf("invalid format for timestamp")
		}

		v, ok := obj["version"].(float64)
		if !ok {
			return false, fmt.Errorf("invalid format for version")
		}

		version := ProcessGroupVersion{
			Version:   v,
			Comments:  obj["comments"].(string),
			Timestamp: time.Unix(int64(t)/1000, 0),
		}

		list = append(list, version)

		return true, nil
	})

	sort.Sort(ByVersion(list))

	return list, nil
}

func (c *Client) SetVersion(versionInfo *VersionControlInfo, revision *Revision, version int) (interface{}, error) {

	if version == versionInfo.Version {
		return nil, nil
	}

	newVersion := *versionInfo
	newVersion.Version = version

	info := &VersionInfo{
		Revision: revision,
		Version:  &newVersion,
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}

	resp, err := c.CallAPI(Post, fmt.Sprintf("/versions/update-requests/process-groups/%v", versionInfo.GroupId), data)
	if err != nil {
		return nil, err
	}

	respUri, err := filter.First(resp, ".request.uri")
	if err != nil {
		return nil, err
	}

	uri, ok := respUri.(string)
	if !ok {
		return nil, fmt.Errorf("invalid uri from update request")
	}

	reqUrl, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	update := NewUpdateRequest(c, reqUrl)
	defer update.Close()

	result, err := update.Wait()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func NewRevision(data interface{}) (*Revision, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("nexpected data type: %v", data)
	}

	revision, ok := m["processGroupRevision"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data type: processGroupRevision=%v", revision)
	}

	clientId, ok := revision["clientId"].(string)
	if !ok {
		clientId = uuid.New().String()
	}

	version, _ := revision["version"].(float64)

	return &Revision{
		ClientId: clientId,
		Version:  int(version),
	}, nil
}

func NewVersionControlInfo(data interface{}) (*VersionControlInfo, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("nexpected data type: %v", data)
	}

	info, ok := m["versionControlInformation"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("the process group hasn't version control")
	}

	groupId, ok := info["groupId"].(string)
	if !ok {
		return nil, fmt.Errorf("process group id not found")
	}

	flowId, ok := info["flowId"].(string)
	if !ok {
		return nil, fmt.Errorf("flow id not found")
	}

	bucketId, ok := info["bucketId"].(string)
	if !ok {
		return nil, fmt.Errorf("bucket id not found")
	}

	registryId, ok := info["registryId"].(string)
	if !ok {
		return nil, fmt.Errorf("registry id not found")
	}

	version, ok := info["version"].(float64)
	if !ok {
		return nil, fmt.Errorf("process group id not found")
	}

	state, ok := info["state"].(string)
	if !ok {
		return nil, fmt.Errorf("process group state not found")
	}

	return &VersionControlInfo{
		GroupId:  groupId,
		Flow:     flowId,
		Bucket:   bucketId,
		Registry: registryId,
		Version:  int(version),
		State:    state,
	}, nil
}
