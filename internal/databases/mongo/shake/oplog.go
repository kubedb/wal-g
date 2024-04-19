package shake

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)

const (
	PrimaryKey = "_id"
)

type CommandOperation struct {
	concernSyncData bool
	runOnAdmin      bool // some commands like `renameCollection` need run on admin database
	needFilter      bool // should be ignored in shake
}

var opsMap = map[string]*CommandOperation{
	"create":           {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"createIndexes":    {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"collMod":          {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"dropDatabase":     {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"drop":             {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"deleteIndex":      {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"deleteIndexes":    {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"dropIndex":        {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"dropIndexes":      {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"renameCollection": {concernSyncData: false, runOnAdmin: true, needFilter: false},
	"convertToCapped":  {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"emptycapped":      {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"applyOps":         {concernSyncData: true, runOnAdmin: false, needFilter: false},
	"startIndexBuild":  {concernSyncData: false, runOnAdmin: false, needFilter: true},
	"commitIndexBuild": {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"abortIndexBuild":  {concernSyncData: false, runOnAdmin: false, needFilter: true},
}

func ExtraCommandName(o bson.D) (string, bool) {
	// command name must be at the first position
	if len(o) > 0 {
		if _, exist := opsMap[o[0].Key]; exist {
			return o[0].Key, true
		}
	}

	return "", false
}

func IsNeedFilterCommand(operation string) bool {
	if op, ok := opsMap[strings.TrimSpace(operation)]; ok {
		return op.needFilter
	}
	return false
}

// Oplog from mongod(5.0) in sharding&replica
// {"ts":{"T":1653449035,"I":3},"v":2,"op":"u","ns":"test.bar",
//  "o":[{"Key":"diff","Value":[{"Key":"d","Value":[{"Key":"ok","Value":false}]},
//                              {"Key":"i","Value":[{"Key":"plus_field","Value":2}]}]}],
//  "o2":[{"Key":"_id","Value":"628da11482387c117d4e9e45"}]}

// "o" : { "$v" : 2, "diff" : { "d" : { "count" : false }, "u" : { "name" : "orange" }, "i" : { "c" : 11 } } }
func DiffUpdateOplogToNormal(updateObj bson.D) (interface{}, error) {

	diffObj := GetKey(updateObj, "diff")
	if diffObj == nil {
		return updateObj, fmt.Errorf("don't have diff field updateObj:[%v]", updateObj)
	}

	bsonDiffObj, ok := diffObj.(bson.D)
	if !ok {
		return updateObj, fmt.Errorf("diff field is not bson.D updateObj:[%v]", updateObj)
	}

	result, err := BuildUpdateDelteOplog("", bsonDiffObj)
	if err != nil {
		return updateObj, fmt.Errorf("parse diffOplog failed updateObj:[%v] err[%v]", updateObj, err)
	}

	return result, nil

}

func BuildUpdateDelteOplog(prefixField string, obj bson.D) (interface{}, error) {
	var result bson.D

	for _, ele := range obj {
		if ele.Key == "d" {
			result = append(result, primitive.E{
				Key:   "$unset",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if ele.Key == "i" || ele.Key == "u" {
			result = append(result, primitive.E{
				Key:   "$set",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if len(ele.Key) > 1 && ele.Key[0] == 's' {
			// s means subgroup field(array or nest)
			tmpPrefixField := ""
			if len(prefixField) == 0 {
				tmpPrefixField = ele.Key[1:]
			} else {
				tmpPrefixField = prefixField + "." + ele.Key[1:]
			}

			nestObj, err := BuildUpdateDelteOplog(tmpPrefixField, ele.Value.(bson.D))
			if err != nil {
				return obj, fmt.Errorf("parse ele[%v] failed, updateObj:[%v]", ele, obj)
			}
			if _, ok := nestObj.(mongo.Pipeline); ok {
				return nestObj, nil
			} else if _, ok := nestObj.(bson.D); ok {
				for _, nestObjEle := range nestObj.(bson.D) {
					result = append(result, nestObjEle)
				}
			} else {
				return obj, fmt.Errorf("unknown nest type ele[%v] updateObj:[%v] nestObj[%v]", ele, obj, nestObj)
			}

		} else if len(ele.Key) > 1 && ele.Key[0] == 'u' {
			result = append(result, primitive.E{
				Key: "$set",
				Value: bson.D{
					primitive.E{
						Key:   prefixField + "." + ele.Key[1:],
						Value: ele.Value,
					},
				},
			})

		} else if ele.Key == "l" {
			if len(result) != 0 {
				return obj, fmt.Errorf("len should be 0, Key[%v] updateObj:[%v], result:[%v]",
					ele, obj, result)
			}

			return mongo.Pipeline{
				{{"$set", bson.D{
					{prefixField, bson.D{
						{"$slice", []interface{}{"$" + prefixField, ele.Value}},
					}},
				}}},
			}, nil

		} else if ele.Key == "a" && ele.Value == true {
			continue
		} else {
			return obj, fmt.Errorf("unknow Key[%v] updateObj:[%v]", ele, obj)
		}
	}

	return result, nil
}

func combinePrefixField(prefixField string, obj interface{}) interface{} {
	if len(prefixField) == 0 {
		return obj
	}

	tmpObj, ok := obj.(bson.D)
	if !ok {
		return obj
	}

	var result bson.D
	for _, ele := range tmpObj {
		result = append(result, primitive.E{
			Key:   prefixField + "." + ele.Key,
			Value: ele.Value})
	}

	return result
}
