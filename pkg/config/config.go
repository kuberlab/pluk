package config

import (
	"fmt"
	"os"

	"github.com/kuberlab/pluk/pkg/utils"
)

func GetConnString() (dbType string, ConnString string) {
	dbType = DBType()
	dbPort := utils.DBPort()
	if dbType == "mysql" {
		if dbPort == "" {
			dbPort = "3306"
		}
		var connString = fmt.Sprintf(
			"%v:%v@tcp(%v)/%v?charset=utf8&parseTime=True&loc=Local",
			utils.DBUser(),
			utils.DBPassword(),
			utils.DBHost(),
			utils.DBName(),
		)
		return dbType, connString
	} else if dbType == "postgres" {
		if dbPort == "" {
			dbPort = "5432"
		}
		connString := fmt.Sprintf(
			"host=%v user=%v dbname=%v sslmode=disable password=%v",
			utils.DBHost(),
			utils.DBUser(),
			utils.DBName(),
			utils.DBPassword(),
		)
		return dbType, connString
	} else if dbType == "sqlite3" {
		// Just path to the filename
		connString := utils.DBName()
		return dbType, connString
	}
	panic("Only mysql/postgres is currently supported")
}

func DBType() string {
	dbType := os.Getenv("DB_TYPE")
	if dbType == "" {
		dbType = "sqlite3"
	}
	return dbType
}
