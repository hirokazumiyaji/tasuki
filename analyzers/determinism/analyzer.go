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
			if lit, ok := unwrapCallbackArg(pass, arg).(*ast.FuncLit); ok {
				excluded[lit] = true
			}
		}
		return true
	})
	return excluded
}

// unwrapCallbackArg resolves the FuncLit passed as a workflow escape-hatch
// callback, unwrapping parenthesized literals and verified function
// conversions such as Callback(func() string {...}) where Callback is a named
// func type. A pure conversion does not execute its argument (unlike the
// factory IIFE hole, which invokes the outer func immediately), so the inner
// literal is still the directly-passed callback and stays exempt. Only
// single-argument CallExprs whose callee resolves to a named func type (not a
// builtin, not a plain func call) unwrap, and the loop continues through
// nested conversions (G(F(func() {...}))) until the literal; IIFEs (Fun is
// itself a FuncLit) and ordinary calls never do, so BadSideEffectFactory
// keeps flagging.
func unwrapCallbackArg(pass *analysis.Pass, e ast.Expr) ast.Expr {
	e = unwrapParen(e)
	for {
		call, ok := e.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return e
		}
		if _, ok := unwrapParen(call.Fun).(*ast.FuncLit); ok {
			return e
		}
		// Unwrap T(x) only when T resolves to a named func type; x may be
		// the FuncLit itself or another such conversion (handled by looping).
		// Anything else — ordinary calls, builtins, IIFE-shaped results —
		// stops the unwrap so the argument keeps being scanned.
		if !isFuncTypeConversion(pass, call) {
			return e
		}
		e = unwrapParen(call.Args[0])
	}
}

// isFuncTypeConversion reports whether call is a conversion to a named func
// type (e.g. Callback(func() {...})) rather than a function invocation.
// The callee must resolve to a *types.TypeName whose underlying type is a
// signature; *types.Func (ordinary calls, methods) and builtins never match.
func isFuncTypeConversion(pass *analysis.Pass, call *ast.CallExpr) bool {
	fun := unwrapParen(call.Fun)
	for {
		switch f := fun.(type) {
		case *ast.IndexExpr:
			fun = unwrapParen(f.X)
		case *ast.IndexListExpr:
			fun = unwrapParen(f.X)
		default:
			goto done
		}
	}
done:
	var obj types.Object
	switch f := fun.(type) {
	case *ast.Ident:
		o, ok := pass.TypesInfo.Uses[f]
		if !ok {
			return false
		}
		if _, ok := o.(*types.Builtin); ok {
			return false
		}
		if _, ok := o.(*types.Func); ok {
			return false
		}
		obj = o
	case *ast.SelectorExpr:
		o, ok := pass.TypesInfo.Uses[f.Sel]
		if !ok {
			return false
		}
		if _, ok := o.(*types.Builtin); ok {
			return false
		}
		if _, ok := o.(*types.Func); ok {
			return false
		}
		obj = o
	default:
		return false
	}
	tn, ok := obj.(*types.TypeName)
	if !ok {
		return false
	}
	_, ok = tn.Type().Underlying().(*types.Signature)
	return ok
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
// and methods defined in those packages. Explicit generic instantiations
// (IndexExpr/IndexListExpr, e.g. workflow.SideEffect[string](...) or
// randv2.N[int](...)) and parenthesized callees (ParenExpr, e.g.
// (time.Sleep)(d)) are unwrapped before resolving the underlying callee.
func resolveCallee(pass *analysis.Pass, call *ast.CallExpr) (pkgPath, funcName string) {
	fun := call.Fun
	for {
		switch f := fun.(type) {
		case *ast.ParenExpr:
			fun = f.X
		case *ast.IndexExpr:
			fun = f.X
		case *ast.IndexListExpr:
			fun = f.X
		default:
			goto resolved
		}
	}
resolved:
	switch fun := fun.(type) {
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
		// Fallback to the qualifier's package name (covers aliases), but
		// only when the selected object is a function: type conversions
		// such as net.IP(raw) or http.Header(values) select a type name,
		// not a *types.Func, and must not be mistaken for package calls
		// into the net branches. Anything else (including unresolvable
		// selectors) is ignored here.
		if pkgIdent, ok := fun.X.(*ast.Ident); ok {
			if obj, ok := pass.TypesInfo.Uses[pkgIdent]; ok {
				if _, ok := obj.(*types.PkgName); ok {
					if selObj, ok := pass.TypesInfo.Uses[fun.Sel]; ok {
						if fn, ok := selObj.(*types.Func); ok && fn.Pkg() != nil {
							return fn.Pkg().Path(), fn.Name()
						}
					}
					return "", ""
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
	// make(chan ...) is channel creation, but only when make resolves to the
	// predeclared builtin: a shadowing local or package-level func (e.g.
	// make := func(chan int) int...; make(ch)) must not match this rule.
	// The callee is unwrapped first so parenthesized builtins such as
	// (make)(chan int) are still recognized.
	if ident, ok := unwrapParen(call.Fun).(*ast.Ident); ok && ident.Name == "make" && len(call.Args) > 0 {
		if obj, ok := pass.TypesInfo.Uses[ident]; ok {
			if _, ok := obj.(*types.Builtin); ok {
				if _, ok := call.Args[0].(*ast.ChanType); ok {
					pass.Reportf(call.Pos(), "make(chan ...) is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
					return
				}
				// make(chan ...) can also hide behind parentheses or a channel type
				// parameter (make(C) with C ~chan int): resolve the argument
				// through the make-specific core type before classifying.
				if tv, ok := pass.TypesInfo.Types[call.Args[0]]; ok && tv.Type != nil {
					if _, ok := coreMakeChanType(tv.Type).(*types.Chan); ok {
						pass.Reportf(call.Pos(), "make(chan ...) is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
						return
					}
				}
			}
		}
	}
	pkgPath, name := resolveCallee(pass, call)
	if name == "" {
		return
	}
	// close/len/cap builtins on channels fall through the package switch
	// below (empty package path). Recognize the predeclared builtins here —
	// TypesInfo.Uses resolves to *types.Builtin only for the real builtin,
	// so a shadowing local or package-level func never matches — and flag
	// channel arguments through the same core-type resolution as make/range
	// (coreMakeChanType covers named channels and channel-constraint type
	// parameters of any direction). len/cap on non-channels (slices, maps,
	// strings) stay clean, and close on a non-channel does not compile, so
	// only channel arguments are ever flagged.
	if pkgPath == "" && len(call.Args) == 1 {
		if ident, ok := unwrapParen(call.Fun).(*ast.Ident); ok {
			if obj, ok := pass.TypesInfo.Uses[ident]; ok {
				if b, ok := obj.(*types.Builtin); ok {
					switch b.Name() {
					case "close", "len", "cap":
						if tv, ok := pass.TypesInfo.Types[call.Args[0]]; ok && tv.Type != nil {
							if _, ok := coreMakeChanType(tv.Type).(*types.Chan); ok {
								pass.Reportf(call.Pos(), "%s on a channel is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await", b.Name())
								return
							}
						}
					}
				}
			}
		}
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
	switch coreRangeType(tv.Type).(type) {
	case *types.Map:
		pass.Reportf(x.Pos(), "ranging over a map is not allowed in workflow code; iteration order is random")
	case *types.Chan:
		pass.Reportf(x.Pos(), "ranging over a channel is not allowed in workflow code; use workflow.Execute/ExecuteAsync and workflow.Await")
	}
}

// embeddedAlternatives returns the alternative type set denoted by one
// embedded constraint term: union members expand per member, exact terms
// (e.g. `C chan int`) resolve to their underlying type, and embedded
// constraint interfaces (named like `Base` or anonymous) resolve to their
// effective (intersected) set. Recursion is guarded by seen with mark/unmark
// so sibling embeds sharing a base do not suppress each other; cycles yield
// no terms (the caller treats term-less embeds as neutral). Non-channel/map terms are kept as-is for the caller to reject.
func embeddedAlternatives(embedded types.Type, seen map[types.Type]bool) []types.Type {
	et := types.Unalias(embedded)
	if union, ok := et.(*types.Union); ok {
		var out []types.Type
		for j := 0; j < union.Len(); j++ {
			mem := types.Unalias(union.Term(j).Type())
			if inner, ok := mem.Underlying().(*types.Interface); ok {
				if seen[mem] {
					continue
				}
				seen[mem] = true
				eff := effectiveConstraintSet(inner, seen)
				delete(seen, mem)
				out = append(out, eff...)
				continue
			}
			out = append(out, mem.Underlying())
		}
		return out
	}
	if inner, ok := et.Underlying().(*types.Interface); ok {
		if seen[et] {
			return nil
		}
		seen[et] = true
		eff := effectiveConstraintSet(inner, seen)
		delete(seen, et)
		return eff
	}
	return []types.Type{et.Underlying()}
}

// effectiveConstraintSet computes the type set denoted by a constraint
// interface as the INTERSECTION across its embedded terms: a candidate type
// survives iff it appears (Identical) in every embedded's alternative set.
// Flattening (union across embeds) is wrong here: e.g.
// `interface { chan int | chan<- int; chan int | <-chan int }` flattens to
// [chan, send, chan, recv] containing a send-only term, while the true set is
// {chan int}. Likewise `interface { chan int | []int; chan int }` flattens to
// a mixed list although only chan int satisfies both embeds. A single embed
// intersects to itself, preserving all prior single-union behavior.
//
// Embeds contributing zero type terms (comparable, method-only interfaces)
// are neutral and skipped: they constrain by method set, not by type terms,
// so they must filter nothing at the core-type level. Treating them as empty
// would erase the intersection (e.g. `interface { ~chan int; comparable }`
// must keep {chan int}). Genuinely empty intersections (e.g. `int` ∩
// `string`) still yield empty via intersectTypeSets below — no value can
// inhabit them, so returning nil (no diagnostic) stays correct there. When no
// embed contributes terms at all, nil is returned as before (unconstrained).
func effectiveConstraintSet(iface *types.Interface, seen map[types.Type]bool) []types.Type {
	var sets [][]types.Type
	for i := 0; i < iface.NumEmbeddeds(); i++ {
		alt := embeddedAlternatives(iface.EmbeddedType(i), seen)
		if len(alt) == 0 {
			continue
		}
		sets = append(sets, alt)
	}
	if len(sets) == 0 {
		return nil
	}
	return intersectTypeSets(sets)
}

// intersectTypeSets returns members of sets[0] present (types.Identical) in
// every other set, deduplicated. Channel direction and element type both
// participate via Identical: `chan int` matches only `chan int`, not
// `<-chan int` or `chan string`.
func intersectTypeSets(sets [][]types.Type) []types.Type {
	var out []types.Type
outer:
	for _, cand := range sets[0] {
		for _, s := range sets[1:] {
			if !containsIdentical(s, cand) {
				continue outer
			}
		}
		if !containsIdentical(out, cand) {
			out = append(out, cand)
		}
	}
	return out
}

func containsIdentical(set []types.Type, t types.Type) bool {
	for _, m := range set {
		if types.Identical(m, t) {
			return true
		}
	}
	return false
}

// embeddedCoreTerms flattens every embedded term of a constraint interface
// into core-relevant types, shared by the range and make checks. It now
// computes the effective INTERSECTED set (see effectiveConstraintSet).
func embeddedCoreTerms(iface *types.Interface) []types.Type {
	return effectiveConstraintSet(iface, map[types.Type]bool{})
}

// coreRangeType resolves the range-relevant type of a range operand. Named
// types (e.g. `type M map[string]int`) never match the Map/Chan switch on
// their own, so non-type-parameter operands are classified by their
// underlying type. A generic type parameter (e.g. M in
// func W[M ~map[string]int](..., m M)) carries the constraint interface as
// its underlying type, so it is resolved to the single underlying type
// shared by its constraint's type set. It returns nil when no single core
// type exists (e.g. a mixed union), in which case ranging would not compile
// anyway.
//
// Channel unions whose members differ only in direction (e.g.
// C chan int | <-chan int) have no single identical core type, yet every
// instantiation ranges over a receivable channel of one element type. When
// all constraint terms are channels permitting receive (chan T or <-chan T)
// with identical element types, the first term is returned so the operand
// still classifies as a channel range. Send-only members never qualify:
// ranging over chan<- T does not compile.
func coreRangeType(t types.Type) types.Type {
	tp, ok := types.Unalias(t).(*types.TypeParam)
	if !ok {
		return types.Unalias(t).Underlying()
	}
	c := tp.Constraint()
	if c == nil {
		return nil
	}
	iface, ok := c.Underlying().(*types.Interface)
	if !ok {
		return nil
	}
	terms := embeddedCoreTerms(iface)
	if len(terms) == 0 {
		return nil
	}
	if elem, ok := recvChanElem(terms[0]); ok {
		recvCompatible := true
		for _, term := range terms[1:] {
			e, ok := recvChanElem(term)
			if !ok || !types.Identical(elem, e) {
				recvCompatible = false
				break
			}
		}
		if recvCompatible {
			return terms[0]
		}
	}
	core := terms[0]
	for _, term := range terms[1:] {
		if !types.Identical(core, term) {
			return nil
		}
	}
	return core
}

// recvChanElem returns the element type when t is a channel permitting
// receive (bidirectional or receive-only); send-only channels and
// non-channels report false.
func recvChanElem(t types.Type) (types.Type, bool) {
	ch, ok := t.(*types.Chan)
	if !ok {
		return nil, false
	}
	if ch.Dir() != types.SendRecv && ch.Dir() != types.RecvOnly {
		return nil, false
	}
	return ch.Elem(), true
}

// coreMakeChanType resolves the channel type created by make(C), where C may
// be a channel type or a type parameter whose constraint is a channel union.
// Unlike coreRangeType (which serves range checks and requires every union
// member to permit receive), make accepts channels of any direction —
// make(chan T), make(<-chan T) and make(chan<- T) all compile — so the union
// path accepts any direction combination as long as every term is a channel
// with an identical element type. It returns nil when C cannot create a
// channel (e.g. a mixed channel/non-channel union), in which case make(C)
// would not compile anyway.
func coreMakeChanType(t types.Type) types.Type {
	tp, ok := types.Unalias(t).(*types.TypeParam)
	if !ok {
		u := types.Unalias(t).Underlying()
		if _, ok := u.(*types.Chan); ok {
			return u
		}
		return nil
	}
	c := tp.Constraint()
	if c == nil {
		return nil
	}
	iface, ok := c.Underlying().(*types.Interface)
	if !ok {
		return nil
	}
	terms := embeddedCoreTerms(iface)
	if len(terms) == 0 {
		return nil
	}
	ch0, ok := terms[0].(*types.Chan)
	if !ok {
		return nil
	}
	elem := ch0.Elem()
	for _, term := range terms[1:] {
		ch, ok := term.(*types.Chan)
		if !ok || !types.Identical(elem, ch.Elem()) {
			return nil
		}
	}
	return terms[0]
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
