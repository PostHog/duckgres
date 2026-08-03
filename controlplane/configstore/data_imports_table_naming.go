package configstore

const (
	DataImportsTableNamingVersionLegacyBatchV1 = "legacy_batch_v1"
	DataImportsTableNamingVersionCopyV1        = "copy_v1"
)

func IsValidDataImportsTableNamingVersion(version string) bool {
	switch version {
	case DataImportsTableNamingVersionLegacyBatchV1, DataImportsTableNamingVersionCopyV1:
		return true
	default:
		return false
	}
}
