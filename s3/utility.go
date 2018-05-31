package s3

import "github.com/aws/aws-sdk-go/service/s3"

func partitionObjects(a []*s3.ObjectIdentifier, b int) [][]*s3.ObjectIdentifier {
	// I've unsuccessfully tried this with interface{} but there was too much of casting
	c := make([][]*s3.ObjectIdentifier, 0)
	for i := 0; i < len(a); i += b {
		if i+b > len(a) {
			c = append(c, a[i:])
		} else {
			c = append(c, a[i:i+b])
		}
	}
	return c
}
