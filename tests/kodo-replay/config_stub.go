package config

var C struct {
	Global struct {
		NsqLatencyThresholdRatio float64
	}
	NSQ struct {
		Lookupd string
	}
}
