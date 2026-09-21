package determinism

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const Doc = `reports non-deterministic calls inside workflow functions (*workflow.Context receivers).

Detected (inside workflow functions):
- go statements
- time.Now/Since/Until/Sleep/After/AfterFunc/NewTimer/NewTicker/Tick
  (use workflow.Now for timestamps, workflow.Sleep for timers)
- math/rand, math/rand/v2, crypto/rand package funcs and methods
  (use workflow.SideEffect or workflow.NewUUID)
- os.Getenv/LookupEnv/Environ/Hostname/Getpid/Getppid/Getwd/Executable and os.Args
  (pass values via inputs or activities)
- sync, sync/atomic, runtime package usage
- net, net/http, os/exec package usage (do I/O in activities)
- channel operations: select statements, channel send (ch <- v) and receive (<-ch),
  make(chan ...) and ranging over a channel (use workflow.Execute/ExecuteAsync and workflow.Await)
- ranging over a map (iteration order is random; collect and sort keys, or range over slices)

Closures passed directly to workflow.SideEffect and workflow.SetQueryHandler are
excluded: they are the official escape hatch (or read-only handler) and may contain
non-deterministic calls. workflow.NewUUID takes no closure argument. Update handlers
(workflow.SetUpdateHandler) are NOT excluded: they are mutating, re-registered on
every replay, and may execute workflow operations.

Not detected (limitations):
- helpers called from a workflow that internally use the above (no interprocedural analysis)
- general package-level variables and interface dispatch
- wall-clock or random values smuggled via workflow inputs already recorded in the journal`

var Analyzer = &analysis.Analyzer{
	Name:     "determinism",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// workflowPkgPath is the canonical import path of the workflow package. Only
// calls resolving to this package qualify for closure exemptions.
const workflowPkgPath = "github.com/hirokazumiyaji/tasuki/workflow"

// sideEffectHandlers are workflow APIs whose FuncLit arguments are escape
// hatches (or handlers) and must not be scanned. The caller must additionally
// verify the callee resolves to workflowPkgPath so that local or third-party
// same-named functions do not evade the analyzer. Matched by selector name so
// aliased and dot imports are also honored.
var sideEffectHandlers = map[string]bool{
	"SideEffect":      true,
	"NewUUID":         true,
	"SetQueryHandler": true,
}

var timeNonDeterministic = map[string]bool{
	"Now":       true,
	"Since":     true,
	"Until":     true,
	"Sleep":     true,
	"After":     true,
	"AfterFunc": true,
	"NewTimer":  true,
	"NewTicker": true,
	"Tick":      true,
}

var osNonDeterministic = map[string]bool{
	"Getenv":     true,
	"LookupEnv":  true,
	"Environ":    true,
	"Hostname":   true,
	"Getpid":     true,
	"Getppid":    true,
	"Getwd":      true,
	"Executable": true,
}

var osVarNonDeterministic = map[string]bool{
	"Args":   true,
	"Stdin":  true,
	"Stdout": true,
	"Stderr": true,
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
		excluded := collectExcludedFuncLits(pass, fn.Body)
		selIdents := collectSelectorIdents(fn.Body)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				if excluded[x] {
					return false
				}
			case *ast.GoStmt:
				pass.Reportf(x.Pos(), "go statement is not allowed in workflow code; use ExecuteAsync/Await")
			case *ast.SelectStmt:
				pass.Reportf(x.Pos(), "select statement is not allowed in workflow code; use workflow.Await")
			case *ast.SendStmt:
				pass.Reportf(x.Pos(), "channel send is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
			case *ast.UnaryExpr:
				if x.Op == token.ARROW {
					pass.Reportf(x.Pos(), "channel receive is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
				}
			case *ast.RangeStmt:
				checkMapRange(pass, x)
			case *ast.CallExpr:
				checkCall(pass, x)
			case *ast.SelectorExpr:
				checkPackageVar(pass, x)
			case *ast.Ident:
				if !selIdents[x] {
					checkDotImportVar(pass, x)
				}
			}
			return true
		})
		return false
	})
	return nil, nil
}

// collectSelectorIdents returns Ident nodes used as SelectorExpr.Sel so the
// generic Ident walk can skip them (they are already handled as selectors).
func collectSelectorIdents(body *ast.BlockStmt) map[*ast.Ident]bool {
	out := map[*ast.Ident]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			out[sel.Sel] = true
		}
		return true
	})
	return out
}

// collectExcludedFuncLits returns FuncLit nodes passed directly as arguments to
// workflow.SideEffect/NewUUID/SetQueryHandler calls. The callee must resolve
// to workflowPkgPath; same-named functions from other packages are not exempt.
// Detection is by function name so aliased imports work.
//
// Only a FuncLit that is itself the callback argument is exempt. Nested
// FuncLits under the argument are NOT exempt: a factory IIFE such as
// func() func() string { ... }() executes immediately (before SideEffect or
// SetQueryHandler runs) and must still be scanned.
func collectExcludedFuncLits(pass *analysis.Pass, body *ast.BlockStmt) map[*ast.FuncLit]bool {
	excluded := map[*ast.FuncLit]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if pkgPath, name := resolveCallee(pass, call); pkgPath != workflowPkgPath || !sideEffectHandlers[name] {
			return true
		}
		for _, arg := range call.Args {
			if lit, ok := unwrapParen(arg).(*ast.FuncLit); ok {
				excluded[lit] = true
			}
		}
		return true
	})
	return excluded
}

// unwrapParen strips parenthesized expressions so ((func() {...}))
// is still recognized as a directly-passed callback.
func unwrapParen(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
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
	return obj.Pkg().Path() == workflowPkgPath && obj.Name() == "Context"
}

// resolveCallee maps a call to its defining package path and function name,
// handling normal, aliased, and dot imports as well as package-level funcs
// and methods defined in those packages.
func resolveCallee(pass *analysis.Pass, call *ast.CallExpr) (pkgPath, funcName string) {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		// Method or package-qualified call: prefer the Func object for Sel.
		if obj, ok := pass.TypesInfo.Uses[fun.Sel]; ok {
			if fn, ok := obj.(*types.Func); ok && fn.Pkg() != nil {
				return fn.Pkg().Path(), fn.Name()
			}
		}
		if sel, ok := pass.TypesInfo.Selections[fun]; ok {
			if fn, ok := sel.Obj().(*types.Func); ok && fn.Pkg() != nil {
				return fn.Pkg().Path(), fn.Name()
			}
		}
		// Fallback to the qualifier's package name (covers aliases).
		if pkgIdent, ok := fun.X.(*ast.Ident); ok {
			if obj, ok := pass.TypesInfo.Uses[pkgIdent]; ok {
				if pkgName, ok := obj.(*types.PkgName); ok {
					return pkgName.Imported().Path(), fun.Sel.Name
				}
			}
		}
		return "", fun.Sel.Name
	case *ast.Ident:
		// Dot imports (and builtins): the Func object carries the package.
		if obj, ok := pass.TypesInfo.Uses[fun]; ok {
			if fn, ok := obj.(*types.Func); ok && fn.Pkg() != nil {
				return fn.Pkg().Path(), fn.Name()
			}
		}
		return "", fun.Name
	default:
		return "", ""
	}
}

func checkCall(pass *analysis.Pass, call *ast.CallExpr) {
	// make(chan ...) is channel creation.
	if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "make" && len(call.Args) > 0 {
		if _, ok := call.Args[0].(*ast.ChanType); ok {
			pass.Reportf(call.Pos(), "make(chan ...) is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
			return
		}
		// make(chan ...) can also hide behind parentheses.
		if tv, ok := pass.TypesInfo.Types[call.Args[0]]; ok {
			if _, ok := tv.Type.Underlying().(*types.Chan); ok {
				pass.Reportf(call.Pos(), "make(chan ...) is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
				return
			}
		}
	}
	pkgPath, name := resolveCallee(pass, call)
	if name == "" {
		return
	}
	switch pkgPath {
	case "time":
		if timeNonDeterministic[name] {
			if name == "Now" || name == "Since" {
				pass.Reportf(call.Pos(), "time.%s is not allowed in workflow code; use workflow.Now", name)
			} else {
				pass.Reportf(call.Pos(), "time.%s is not allowed in workflow code; use workflow.Sleep or workflow.Now", name)
			}
		}
	case "math/rand", "math/rand/v2", "crypto/rand":
		pass.Reportf(call.Pos(), "%s.%s is not allowed in workflow code; use workflow.SideEffect or NewUUID", pkgPath, name)
	case "os":
		if osNonDeterministic[name] {
			pass.Reportf(call.Pos(), "os.%s is not allowed in workflow code; pass values via inputs or activities", name)
		}
	case "sync", "sync/atomic":
		pass.Reportf(call.Pos(), "%s.%s is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await", pkgPath, name)
	case "runtime":
		pass.Reportf(call.Pos(), "runtime.%s is not allowed in workflow code", name)
	case "net", "net/http", "os/exec":
		pass.Reportf(call.Pos(), "%s.%s is not allowed in workflow code; do I/O in activities", pkgPath, name)
	}
}

func checkMapRange(pass *analysis.Pass, x *ast.RangeStmt) {
	if x.X == nil {
		return
	}
	tv, ok := pass.TypesInfo.Types[x.X]
	if !ok || tv.Type == nil {
		return
	}
	switch tv.Type.Underlying().(type) {
	case *types.Map:
		pass.Reportf(x.Pos(), "ranging over a map is not allowed in workflow code; iteration order is random")
	case *types.Chan:
		pass.Reportf(x.Pos(), "ranging over a channel is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
	}
}

// checkPackageVar flags os.Args (and Stdin/Stdout/Stderr) selector accesses.
// Func references are ignored here; they are handled by checkCall.
func checkPackageVar(pass *analysis.Pass, x *ast.SelectorExpr) {
	if obj, ok := pass.TypesInfo.Uses[x.Sel]; ok {
		if v, ok := obj.(*types.Var); ok && v.Pkg() != nil {
			if v.Pkg().Path() == "os" && osVarNonDeterministic[v.Name()] {
				pass.Reportf(x.Pos(), "os.%s is not allowed in workflow code; pass values via inputs or activities", v.Name())
			}
		}
	}
}

// checkDotImportVar flags dot-imported os.Args style identifiers.
func checkDotImportVar(pass *analysis.Pass, x *ast.Ident) {
	obj, ok := pass.TypesInfo.Uses[x]
	if !ok {
		return
	}
	v, ok := obj.(*types.Var)
	if !ok || v.Pkg() == nil {
		return
	}
	if v.Pkg().Path() == "os" && osVarNonDeterministic[v.Name()] {
		pass.Reportf(x.Pos(), "os.%s is not allowed in workflow code; pass values via inputs or activities", v.Name())
	}
}
