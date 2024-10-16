package objectstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
)

func createS3Client(region, accessKey, secretKey string) (*s3.S3, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
		Credentials: credentials.NewStaticCredentials(
			accessKey, secretKey, "",
		),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	s3Client := s3.New(sess)
	return s3Client, nil
}

type S3Bucket struct {
	client *s3.S3
	bucket string
	key    string
}

// AddTags is an additional method for S3Bucket.
func (b *S3Bucket) AddTags(ctx context.Context, tags []*s3.Tag) (err error) {
	defer func() {
		if err != nil {
			// wrap error before returning
			err = fmt.Errorf("failed to add tags to S3 bucket: %w", err)
		}
	}()
	glog.Info("Bucket String: ", b.bucket)
	bucket := aws.String(b.bucket)
	glog.Info("Key String: ", b.key)
	key := aws.String(b.key)
	glog.Info("Tags: ", tags)
	s3Tags := s3.Tagging{TagSet: tags}

	if b.client == nil {
		return fmt.Errorf("S3 client is not initialized")
	}
	if b.bucket == "" {
		return fmt.Errorf("bucket name is not set")
	}
	if b.key == "" {
		return fmt.Errorf("object key is not set")
	}

	glog.Info("Calling PutObjectTagging")
	objectTaggingInput := &s3.PutObjectTaggingInput{
		Bucket:  bucket,
		Key:     key,
		Tagging: &s3Tags,
	}

	glog.Info("Calling PutObjectTaggingWithContext")
	// sess, err1 := createS3BucketSession(ctx, namespace, config.SessionInfo, k8sClient)
	_, err = b.client.PutObjectTaggingWithContext(ctx, objectTaggingInput)
	if err != nil {
		glog.Errorf("failed to add tags to S3 bucket: %v", err)
		return fmt.Errorf("failed to add tags to S3 bucket: %w", err)
	}
	return err
}
