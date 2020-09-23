package db

import (
	"crypto/sha1"
	"fmt"
)

type AuthMgr interface {
	CreateAuth(key string) error
	GetAuth(key string) (*Auth, error)
	//ListAuths(filter Auth) ([]*Auth, error)
	//DeleteAuth(id uint) error
}

type Auth struct {
	BaseModel
	//ID   uint   `json:"id" sql:"AUTO_INCREMENT"`
	Hash string `json:"hash" gorm:"primary_key"`
}

func authHash(key string) string {
	hash := sha1.Sum([]byte(key))
	return fmt.Sprintf("%x", hash[:])
}

func (mgr *DatabaseMgr) CreateAuth(key string) error {
	hash := authHash(key)
	if mgr.DBType() == "postgres" {
		tpl := "INSERT INTO auths (hash) VALUES (?) ON CONFLICT (hash) DO NOTHING"
		err := mgr.db.Exec(tpl, hash).Error
		if err != nil {
			return err
		}
		return nil

	} else if mgr.DBType() == "sqlite3" {
		tpl := "INSERT INTO auths (hash) VALUES (?) ON CONFLICT (hash) DO NOTHING"
		err := mgr.db.Exec(tpl, hash).Error
		if err != nil {
			return err
		}
		return nil
	} else {
		return mgr.db.Create(&Auth{Hash: hash}).Error
	}
}

func (mgr *DatabaseMgr) UpdateAuth(Auth *Auth) (*Auth, error) {
	err := mgr.db.Save(Auth).Error
	return Auth, err
}

func (mgr *DatabaseMgr) GetAuth(key string) (*Auth, error) {
	var auth = Auth{}

	err := mgr.db.First(&auth, Auth{Hash: authHash(key)}).Error
	return &auth, err
}

//func (mgr *DatabaseMgr) DeleteAuth(id uint) error {
//	return mgr.db.Delete(Auth{}, Auth{ID: id}).Error
//}
