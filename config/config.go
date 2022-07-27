package config

import (
	"os"
	"strconv"
)

func GetS3Bucket() string {
	return os.Getenv("S3_BUCKET")
}

func GetS3PathFormat() string {
	return os.Getenv("S3_PATH_FORMAT")
}

func GetDatabaseName() string {
	return os.Getenv("DATABASE_NAME")
}

func OnlyThisMonth() bool {
	onlyThisMonth, _ := strconv.ParseBool(os.Getenv("ONLY_THIS_MONTH"))
	return onlyThisMonth
}
