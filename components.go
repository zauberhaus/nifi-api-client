package nifi

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

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
