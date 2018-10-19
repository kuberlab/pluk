package main

import (
	"fmt"
	"github.com/kuberlab/pluk/pkg/plukclient"
)

var (
	defaultEntityType = "dataset"
)

type EntityType struct {
	Value string
}

func (e *EntityType) String() string {
	return e.Value
}

func (e *EntityType) Set(value string) error {
	if value == "" {
		value = defaultEntityType
	}

	_, ok := plukclient.AllowedTypes[value]
	if !ok {
		return fmt.Errorf("Allowed values for entityType: %v", plukclient.AllowedTypesList())
	}
	e.Value = value
	return nil
}

func (e *EntityType) Type() string {
	return "entityType"
}
