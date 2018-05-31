package s3

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

func partitionToObjects(keys []string) []*s3.ObjectIdentifier {
	objs := make([]*s3.ObjectIdentifier, len(keys))
	for i, k := range keys {
		objs[i] = &s3.ObjectIdentifier{Key: aws.String(k)}
	}
	return objs
}