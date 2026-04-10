package protocol

const (
    CmdCreate      = "CREATE"
    CmdDelete      = "DELETE"
    CmdPublish     = "PUBLISH"
    CmdSubscribe   = "SUBSCRIBE"
    CmdUnsubscribe = "UNSUBSCRIBE"
    CmdAck         = "ACK"
    CmdPing        = "PING"
)

const (
    TypeQueue = "QUEUE"
    TypeTopic = "TOPIC"
)

const (
    RespOk  = "+OK"
    RespErr = "-ERR"
)