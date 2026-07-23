package main

import (
	"github.com/hirokazumiyaji/tasuki/analyzers/determinism"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() { singlechecker.Main(determinism.Analyzer) }
