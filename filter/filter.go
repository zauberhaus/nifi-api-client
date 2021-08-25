package filter

import (
	"encoding/json"

	"github.com/itchyny/gojq"
)

func First(input interface{}, query string) (interface{}, error) {
	var result interface{}

	err := Map(input, query, func(val interface{}) (bool, error) {
		result = val
		return false, nil
	})

	return result, err
}

func FirstQuery(input interface{}, query *gojq.Query) (interface{}, error) {
	var result interface{}

	err := MapQuery(input, query, func(val interface{}) (bool, error) {
		result = val
		return false, nil
	})

	return result, err
}

func List(input interface{}, query string) ([]interface{}, error) {

	result := []interface{}{}

	err := Map(input, query, func(val interface{}) (bool, error) {
		result = append(result, val)
		return true, nil
	})

	return result, err
}

func ListQuery(input interface{}, query *gojq.Query) ([]interface{}, error) {

	result := []interface{}{}

	err := MapQuery(input, query, func(val interface{}) (bool, error) {
		result = append(result, val)
		return true, nil
	})

	return result, err
}

func Map(input interface{}, query string, f func(val interface{}) (bool, error)) error {
	q, err := gojq.Parse(query)
	if err != nil {
		return err
	}

	return MapQuery(input, q, f)
}

func MapQuery(input interface{}, query *gojq.Query, f func(val interface{}) (bool, error)) error {

	if data, ok := input.(string); ok {
		var i interface{}
		err := json.Unmarshal([]byte(data), &i)
		if err != nil {
			return err
		}

		input = i
	} else if data, ok := input.([]byte); ok {
		var i interface{}
		err := json.Unmarshal(data, &i)
		if err != nil {
			return err
		}

		input = i
	}

	iter := query.Run(input)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return err
		}

		if v != nil {
			cont, err := f(v)

			if err != nil {
				return err
			}

			if !cont {
				break
			}
		}
	}

	return nil
}

func Check(input interface{}, query string, expected interface{}) (bool, error) {
	found := false
	result := true

	err := Map(input, query, func(val interface{}) (bool, error) {
		found = true
		result = result && val == expected
		return result, nil
	})

	return result && found, err
}
