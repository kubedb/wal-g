package shake

import (
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

func FindFiledPrefix(input bson.D, prefix string) bool {
	for id := range input {
		if strings.HasPrefix(input[id].Key, prefix) {
			return true
		}
	}

	return false
}

func GetKey(obj bson.D, wanted string) interface{} {
	ret, _ := GetKeyWithIndex(obj, wanted)
	return ret
}

func GetKeyWithIndex(obj bson.D, wanted string) (interface{}, int) {
	if wanted == "" {
		wanted = PrimaryKey
	}

	// "_id" is always the first field
	for id, ele := range obj {
		if ele.Key == wanted {
			return ele.Value, id
		}
	}

	return nil, 0
}
