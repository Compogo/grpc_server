package stream

import "errors"

var (
	MessageChanFullError   = errors.New("message channel is full")
	MessageChanClosedError = errors.New("message channel is closed")
	SendMessageError       = errors.New("send message error")
)
