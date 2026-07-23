package determinism_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/analyzers/determinism"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), determinism.Analyzer, "a")
}
