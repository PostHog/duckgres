package pgtypes

import (
	"regexp"
	"strconv"
	"strings"
)

type MetadataType struct {
	DataType         string
	UDTName          string
	NumericPrecision *int
	NumericScale     *int
}

var icebergDecimalRE = regexp.MustCompile(`^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)

func ForIceberg(raw string) MetadataType {
	t := strings.ToLower(strings.TrimSpace(raw))
	if strings.HasPrefix(t, "{") {
		return MetadataType{DataType: "jsonb", UDTName: "jsonb"}
	}
	if m := icebergDecimalRE.FindStringSubmatch(t); m != nil {
		precision, _ := strconv.Atoi(m[1])
		scale, _ := strconv.Atoi(m[2])
		return MetadataType{
			DataType:         "numeric",
			UDTName:          "numeric",
			NumericPrecision: &precision,
			NumericScale:     &scale,
		}
	}
	if strings.HasPrefix(t, "fixed") {
		return MetadataType{DataType: "bytea", UDTName: "bytea"}
	}
	switch t {
	case "boolean", "bool":
		return MetadataType{DataType: "boolean", UDTName: "bool"}
	case "int", "integer":
		return MetadataType{DataType: "integer", UDTName: "int4"}
	case "long", "bigint":
		return MetadataType{DataType: "bigint", UDTName: "int8"}
	case "float":
		return MetadataType{DataType: "real", UDTName: "float4"}
	case "double":
		return MetadataType{DataType: "double precision", UDTName: "float8"}
	case "date":
		return MetadataType{DataType: "date", UDTName: "date"}
	case "time":
		return MetadataType{DataType: "time without time zone", UDTName: "time"}
	case "timestamp", "timestamp_ns":
		return MetadataType{DataType: "timestamp without time zone", UDTName: "timestamp"}
	case "timestamptz", "timestamptz_ns", "timestamp_tz":
		return MetadataType{DataType: "timestamp with time zone", UDTName: "timestamptz"}
	case "string":
		return MetadataType{DataType: "text", UDTName: "text"}
	case "uuid":
		return MetadataType{DataType: "uuid", UDTName: "uuid"}
	case "binary":
		return MetadataType{DataType: "bytea", UDTName: "bytea"}
	case "struct", "list", "map", "variant":
		return MetadataType{DataType: "jsonb", UDTName: "jsonb"}
	default:
		return MetadataType{DataType: t, UDTName: t}
	}
}
