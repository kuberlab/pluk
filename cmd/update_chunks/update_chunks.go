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
		sql := fmt.Sprintf(`UPDATE chunks SET size=%v WHERE hash='%v'`, size, hash)
		err = tx.Exec(sql).Error
		//cmd := exec.Command("sqlite3", "/pluk/pluke.db", sql)
		//out, err := cmd.CombinedOutput()
		if err != nil {
			log.Println(err)
			return err
		}
		fmt.Print(".")

		return nil
	})
	if err != nil {
		log.Println(err)
		return
	}
}
