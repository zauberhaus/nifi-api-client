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

type NiFiType byte

const (
	ProcessGroup         NiFiType = 1 << iota
	RemoteProcessGroup   NiFiType = 1 << iota
	Processor            NiFiType = 1 << iota
	Connection           NiFiType = 1 << iota
	InputPort            NiFiType = 1 << iota
	OutputPort           NiFiType = 1 << iota
	UnknownType          NiFiType = 1 << iota
	AllTypes             NiFiType = 0xff
	AllExceptConnections NiFiType = 0xf7
)

const (
	ProcessGroupName       = "processgroup"
	RemoteProcessGroupName = "remoteprocessgroup"
	ProcessorName          = "processor"
	ConnectionName         = "connection"
	InputPortName          = "inputport"
	OutputPortName         = "outputport"
)

const (
	ProcessGroupTitle       = "Process Group"
	RemoteProcessGroupTitle = "Remote Process Group"
	ProcessorTitle          = "Processor"
	ConnectionTitle         = "Connection"
	InputPortTitle          = "Input Port"
	OutputPortTitle         = "Output Port"
)

func (t NiFiType) String() string {
	switch t {
	case ProcessGroup:
		return ProcessGroupTitle
	case Processor:
		return ProcessorTitle
	case RemoteProcessGroup:
		return RemoteProcessGroupTitle
	case Connection:
		return ConnectionTitle
	case InputPort:
		return InputPortTitle
	case OutputPort:
		return OutputPortTitle
	}

	return "unknown"
}

type Component struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Path       string                 `json:"path"`
	Type       NiFiType               `json:"-"`
	TypeName   string                 `json:"type"`
	Attributes map[string]interface{} `json:"-"`
}

func (c *Component) String() string {
	if len(c.Name) == 0 {
		return "(" + c.TypeName + ")"
	} else {
		return c.Name + " (" + c.TypeName + ")"
	}
}

type ByType []*Component

func (a ByType) Len() int           { return len(a) }
func (a ByType) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByType) Less(i, j int) bool { return a[i].Type < a[j].Type }

func NewComponent(t string, p string, o map[string]interface{}) *Component {
	id := ""
	name := ""
	tp := UnknownType
	tn := "unknown"

	val, ok := o["id"]
	if ok {
		txt, ok := val.(string)
		if ok {
			id = txt
		}
	}

	val, ok = o["name"]
	if ok {
		txt, ok := val.(string)
		if ok {
			name = txt
		} else {
			name = "??"
		}
	} else {
		name = "?"
	}

	switch t {
	case ProcessGroupName:
		tp = ProcessGroup
		tn = ProcessGroupTitle
	case ProcessorName:
		tp = Processor
		tn = ProcessorTitle
	case RemoteProcessGroupName:
		tp = RemoteProcessGroup
		tn = RemoteProcessGroupTitle
	case ConnectionName:
		tp = Connection
		tn = ConnectionTitle
	case InputPortName:
		tp = InputPort
		tn = InputPortTitle
	case OutputPortName:
		tp = OutputPort
		tn = OutputPortTitle
	}

	return &Component{
		ID:         id,
		Name:       name,
		Type:       tp,
		TypeName:   tn,
		Path:       p,
		Attributes: o,
	}
}
