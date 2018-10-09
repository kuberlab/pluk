package db

import (
	"testing"

	"github.com/kuberlab/pluk/pkg/utils"
)

func setup() {
	DbMgr = NewFakeDatabaseMgr(":memory:")
}

func teardown() {
	DbMgr.Close()
}

func TestCreateDataset(t *testing.T) {
	setup()
	defer teardown()
	ds := &Dataset{
		Workspace: "workspace",
		Name:      "dataset",
	}
	err := DbMgr.CreateDataset(ds)
	if err != nil {
		t.Fatal(err)
	}

	utils.Assert(true, ds.ID != 0, t)
}
