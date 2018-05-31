package walg

import (
	"fmt"
	"reflect"
	"sync"
)

type Operator interface {
	GetTarUploader() (TarUploaderInterface, error)
	GetCloud() (Cloud, error)
}

var operatorsRegistry struct {
	v map[string]Operator
	sync.Mutex
}

func init() {
	operatorsRegistry.v = make(map[string]Operator)
}

// RegisterOperator register new Operator
func RegisterOperator(name string, operator Operator) {
	operatorsRegistry.Lock()
	operatorsRegistry.v[name] = operator
	operatorsRegistry.Unlock()
}

// NewOperator return registered Operator
func NewOperator(name string) (Operator, error) {
	operatorsRegistry.Lock()
	value, ok := operatorsRegistry.v[name]
	operatorsRegistry.Unlock()
	if !ok {
		return nil, fmt.Errorf("operator not fount for %s", name)
	}
	operator := reflect.New(reflect.TypeOf(value).Elem()).Interface().(Operator)
	return operator, nil
}
