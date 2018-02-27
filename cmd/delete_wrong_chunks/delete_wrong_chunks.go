package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

type Answer struct {
	Size int64 `json:"size"`
}

func main() {
	db, err := gorm.Open("sqlite3", "/pluk/pluke.db")
	if err != nil {
		log.Fatal(err)
	}
	tx := db.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	err = filepath.Walk("/data", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		hash := strings.TrimPrefix(path, "/data/")
		hash = strings.Replace(hash, "/", "", -1)

		size := info.Size()
		answer := Answer{}
		sql := fmt.Sprintf(`SELECT size from chunks WHERE hash='%v'`, hash)
		err = tx.Raw(sql).Scan(&answer).Error
		//cmd := exec.Command("sqlite3", "/pluk/pluke.db", sql)
		//out, err := cmd.CombinedOutput()
		if err == gorm.ErrRecordNotFound {
			// Extra chunk / unneeded.
			os.Remove(path)
			fmt.Println(path)
			return nil
		}
		if err != nil {
			log.Println(err)
			return err
		}
		if answer.Size != 0 && answer.Size != size {
			os.Remove(path)
			fmt.Println(path)
		}

		return nil
	})
	if err != nil {
		log.Println(err)
		return
	}
}
