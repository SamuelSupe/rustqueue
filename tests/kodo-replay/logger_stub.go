package nsq

type replayLogger struct{}

func (replayLogger) Warnf(string, ...any)  {}
func (replayLogger) Infof(string, ...any)  {}
func (replayLogger) Errorf(string, ...any) {}

var l replayLogger
