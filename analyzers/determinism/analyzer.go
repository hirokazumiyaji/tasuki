package determinism

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const Doc = `reports non-deterministic calls inside workflow functions (*workflow.Context receivers).

Flags time.Now / time.Since, go statements, and math/rand or crypto/rand usage.`

var Analyzer = &analysis.Analyzer{
	Name:     "determinism",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{(*ast.FuncDecl)(nil)}
	insp.Nodes(nodeFilter, func(n ast.Node, push bool) bool {
		if !push {
			return false
		}
		fn := n.(*ast.FuncDecl)
		if fn.Body == nil || !isWorkflowFunc(pass, fn) {
			return false
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.GoStmt:
				pass.Reportf(x.Pos(), "go statement is not allowed in workflow code; use ExecuteAsync/Await")
			case *ast.CallExpr:
				checkCall(pass, x)
			}
			return true
		})
		return false
	})
	return nil, nil
}

func isWorkflowFunc(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	if fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
		return false
	}
	for _, field := range fn.Type.Params.List {
		if isWorkflowContextType(pass, field.Type) {
			return true
		}
	}
	return false
}

func isWorkflowContextType(pass *analysis.Pass, expr ast.Expr) bool {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	tv, ok := pass.TypesInfo.Types[star]
	if !ok {
		return false
	}
	ptr, ok := tv.Type.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	return obj.Pkg().Path() == "github.com/hirokazumiyaji/tasuki/workflow" && obj.Name() == "Context"
}

func checkCall(pass *analysis.Pass, call *ast.CallExpr) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return
	}
	obj, ok := pass.TypesInfo.Uses[pkgIdent]
	if !ok {
		return
	}
	pkgName, ok := obj.(*types.PkgName)
	if !ok {
		return
	}
	path := pkgName.Imported().Path()
	name := sel.Sel.Name
	switch path {
	case "time":
		if name == "Now" || name == "Since" {
			pass.Reportf(call.Pos(), "time.%s is not allowed in workflow code; use workflow.Now", name)
		}
	case "math/rand", "math/rand/v2", "crypto/rand":
		pass.Reportf(call.Pos(), "%s.%s is not allowed in workflow code; use workflow.SideEffect or NewUUID", path, name)
	}
}
