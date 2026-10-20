package aws

import (
	"github.com/go-logr/logr"
)

func testLogger() logr.Logger { return logr.Discard() }
