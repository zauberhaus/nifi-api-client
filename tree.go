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
	"strconv"
	"strings"
)

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
