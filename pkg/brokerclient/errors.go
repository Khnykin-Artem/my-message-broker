package brokerclient

import "errors"

var (
    ErrNotConnected      = errors.New("not connected to broker")
    ErrAuthenticationRequired = errors.New("authentication required")
    ErrInvalidResponse   = errors.New("invalid response from broker")
    ErrTimeout           = errors.New("operation timeout")
    ErrSubscriberClosed  = errors.New("subscriber closed")
)