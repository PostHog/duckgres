package properties

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Discover lists the Parquet files directly below the configured dataset prefix,
// including nested directories, just as the frozen-data registration does.
func Discover(ctx context.Context, client S3Client, uri string) (*Dataset, error) {
	u, err := s3URL(uri)
	if err != nil {
		return nil, err
	}
	prefix := strings.TrimRight(strings.TrimPrefix(u.Path, "/"), "/") + "/"
	dataset := &Dataset{Prefix: "s3://" + u.Host + "/" + prefix}
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(u.Host), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list properties dataset: %w", err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if !strings.HasSuffix(key, ".parquet") {
				continue
			}
			if aws.ToInt64(object.Size) <= 0 {
				return nil, fmt.Errorf("properties dataset contains an empty Parquet object")
			}
			dataset.Files = append(dataset.Files, File{Key: key, Size: aws.ToInt64(object.Size), ETag: aws.ToString(object.ETag)})
		}
	}
	if len(dataset.Files) == 0 {
		return nil, fmt.Errorf("properties dataset contains no Parquet files")
	}
	sort.Slice(dataset.Files, func(i, j int) bool { return dataset.Files[i].Key < dataset.Files[j].Key })
	inventory, err := json.Marshal(struct {
		Prefix string
		Files  []File
	}{dataset.Prefix, dataset.Files})
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(inventory)
	dataset.SHA256 = hex.EncodeToString(digest[:])
	return dataset, nil
}

type GlueClient interface {
	GetTable(context.Context, *glue.GetTableInput, ...func(*glue.Options)) (*glue.GetTableOutput, error)
}

// VerifyAthenaTable checks the preprovisioned logical projection without mutating Glue.
func (m *Dataset) VerifyAthenaTable(ctx context.Context, client GlueClient, database, table string) error {
	out, err := client.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String(database), Name: aws.String(table)})
	if err != nil {
		return fmt.Errorf("verify Athena table: %w", err)
	}
	if out.Table == nil || out.Table.StorageDescriptor == nil {
		return fmt.Errorf("athena table lacks storage descriptor")
	}
	sd := out.Table.StorageDescriptor
	if strings.EqualFold(out.Table.Parameters["projection.enabled"], "true") {
		return fmt.Errorf("athena partition projection must be disabled")
	}
	if sd.SerdeInfo == nil || aws.ToString(sd.SerdeInfo.SerializationLibrary) != "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" {
		return fmt.Errorf("athena table requires standard Parquet SerDe")
	}

	if aws.ToString(sd.Location) != m.Prefix {
		return fmt.Errorf("athena table location differs from fixture")
	}
	if strings.EqualFold(out.Table.Parameters["parquet.column.index.access"], "true") || strings.EqualFold(sd.SerdeInfo.Parameters["parquet.column.index.access"], "true") {
		return fmt.Errorf("athena table must read Parquet columns by name")
	}
	required := map[string]string{"event": "string", "timestamp": "timestamp", "properties": "string", "properties_typed": "struct<$browser:string>"}
	if len(sd.Columns) != len(required) || len(out.Table.PartitionKeys) != 0 {
		return fmt.Errorf("athena table must expose exactly the supported unpartitioned columns")
	}
	for _, c := range sd.Columns {
		name := aws.ToString(c.Name)
		typ := strings.ReplaceAll(strings.ToLower(aws.ToString(c.Type)), "`", "")
		if required[name] != typ || typ == "" {
			return fmt.Errorf("athena table logical column mismatch")
		}
		delete(required, name)
	}
	if len(required) != 0 || aws.ToString(sd.InputFormat) != "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" || aws.ToString(sd.OutputFormat) != "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" {
		return fmt.Errorf("athena table must use the supported Parquet schema")
	}
	return nil
}
