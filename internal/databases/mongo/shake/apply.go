package shake

import (
	"context"
	"github.com/mongodb/mongo-tools-common/db"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)

const (
	versionMark = "$v"
	uuidMark    = "ui"
)

func ApplyOplogForShard(op *db.Oplog, dbClient *mongo.Client) error {
	ns := strings.SplitN(op.Namespace, ".", 2)
	switch op.Operation {
	case "i":

	}
}

// { "op" : "i", "ns" : "test.c", "ui" : UUID("4654d08e-db1f-4e94-9778-90aeee4feff0"), "o" : { "_id" : ObjectId("627a1f83b95fae5fca006bac"), "a" : 1, "b" : 1, "c" : 1 }, "ts" : Timestamp(1652170627, 2), "t" : NumberLong(1), "wall" : ISODate("2022-05-10T08:17:07.558Z"), "v" : NumberLong(2) }
func doInsert(op *db.Oplog, client *mongo.Client, ns []string) error {
	collectionHandle := client.Database(ns[0]).Collection(ns[1])
	_, err := collectionHandle.InsertOne(context.Background(), op.Object)
	if err != nil {
		return err
	}
	return nil
}

/*
replace(update all):

	db.c.insert({"a":1,"b":1,"c":1}) + db.c.update({"a":1}, {"b":2})
	{ "op" : "u", "ns" : "test.c", "ui" : UUID("4654d08e-db1f-4e94-9778-90aeee4feff0"), "o" : { "_id" : ObjectId("627a2492b95fae5fca006bad"), "b" : 2 }, "o2" : { "_id" : ObjectId("627a2492b95fae5fca006bad") }, "ts" : Timestamp(1652171939, 1), "t" : NumberLong(1), "wall" : ISODate("2022-05-10T08:38:59.701Z"), "v" : NumberLong(2) }

updateOne:

	db.c.insert({"a":1,"b":1,"c":1}) + db.c.updateOne({"a":1}, {"$set":{"b":2}})
	{ "op" : "u", "ns" : "test.c", "ui" : UUID("4654d08e-db1f-4e94-9778-90aeee4feff0"), "o" : { "$v" : 1, "$set" : { "b" : 3 }, "$unset" : { "c" : true } }, "o2" : { "_id" : ObjectId("627a1f83b95fae5fca006bac") }, "ts" : Timestamp(1652170892, 1), "t" : NumberLong(1), "wall" : ISODate("2022-05-10T08:21:32.695Z"), "v" : NumberLong(2) }
*/
func doUpdate(op *db.Oplog, client *mongo.Client, ns []string) error {
	collectionHandle := client.Database(ns[0]).Collection(ns[1])

	var update interface{}
	var err error
	var res *mongo.UpdateResult
	if FindFiledPrefix(op.Object, "$") {
		var oplogErr error
		oplogVer, ok := GetKey(op.Object, versionMark).(int32)
		if ok && oplogVer == 2 {
			if update, oplogErr = DiffUpdateOplogToNormal(op.Object); oplogErr != nil {
				return oplogErr
			}
		}
	}
}
