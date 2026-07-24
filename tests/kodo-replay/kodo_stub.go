package kodo

import "time"

type BoolCommand struct{}

func (*BoolCommand) Result() (bool, error) {
	return false, nil
}

type IntCommand struct{}

type RedisStore interface {
	SetNX(string, any, time.Duration) *BoolCommand
	Del(...string) *IntCommand
}

var Redis RedisStore
