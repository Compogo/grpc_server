package stream

import "errors"

var (
	// MessageChanFullError возникает, когда буфер сообщений заполнен.
	MessageChanFullError = errors.New("message channel is full")

	// MessageChanClosedError возникает при попытке записи в закрытый канал.
	MessageChanClosedError = errors.New("message channel is closed")

	// SendMessageError возникает при ошибке отправки сообщения.
	SendMessageError = errors.New("send message error")
)
