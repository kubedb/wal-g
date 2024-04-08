package shake

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/wal-g/tracelog"
	"reflect"
	"strings"
)

// OplogFilter: AutologousFilter, NoopFilter, DDLFilter
type OplogFilter interface {
	Filter(log *db.Oplog) bool
}
type OplogFilterChain []OplogFilter

func (chain OplogFilterChain) IterateFilter(log *db.Oplog) bool {
	for _, filter := range chain {
		if filter.Filter(log) {
			tracelog.InfoLogger.Printf("%v filter oplog[%v]", reflect.TypeOf(filter), log)
			return true
		}
	}
	return false
}

type AutologousFilter struct {
}

func (filter *AutologousFilter) Filter(log *db.Oplog) bool {

	// Filter out unnecessary commands
	if operation, found := ExtraCommandName(log.Object); found {
		fmt.Printf("unnecessary commands. operation: %v found: %v\n", operation)
		if IsNeedFilterCommand(operation) {
			return true
		}
	}

	// for namespace. we filter noop operation and collection name
	// that are admin, local, mongoshake, mongoshake_conflict
	return filter.FilterNs(log.Namespace)
}

// namespace should be filtered.
// key: ns, value: true means prefix, false means contain
var NsShouldBeIgnore = map[string]bool{
	"admin.":       true,
	"local.":       true,
	"config.":      true,
	"system.views": false,
}

// namespace should not be filtered.
// NsShouldNotBeIgnore has a higher priority than NsShouldBeIgnore
// key: ns, value: true means prefix, false means contain
var NsShouldNotBeIgnore = map[string]bool{
	"admin.$cmd": true,
}

func (filter *AutologousFilter) FilterNs(namespace string) bool {
	// for namespace. we filter noop operation and collection name
	// that are admin, local, config, mongoshake, mongoshake_conflict

	// v2.4.13, don't filter admin.$cmd which may include transaction
	for key, val := range NsShouldNotBeIgnore {
		if val == true && strings.HasPrefix(namespace, key) {
			return false
		}
		if val == false && strings.Contains(namespace, key) {
			return false
		}
	}

	for key, val := range NsShouldBeIgnore {
		if val == true && strings.HasPrefix(namespace, key) {
			return true
		}
		if val == false && strings.Contains(namespace, key) {
			return true
		}
	}
	return false
}

type NoopFilter struct {
}

func (filter *NoopFilter) Filter(log *db.Oplog) bool {
	return log.Operation == "n"
}

type DDLFilter struct {
}

func (filter *DDLFilter) Filter(log *db.Oplog) bool {
	operation, _ := ExtraCommandName(log.Object)
	return log.Operation == "c" && operation != "applyOps" && operation != "create" || strings.HasSuffix(log.Namespace, "system.indexes")
}
