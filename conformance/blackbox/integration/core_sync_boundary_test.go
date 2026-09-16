package integration

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

var coreSyncEntrypoints = []string{"serveConnect", "servePush", "servePull", "serveRebuild"}

var allowedCoreHTTPSelectors = map[string]bool{
	"MethodPost":                true,
	"NoBody":                    true,
	"Request":                   true,
	"ResponseWriter":            true,
	"StatusBadRequest":          true,
	"StatusConflict":            true,
	"StatusInternalServerError": true,
	"StatusMethodNotAllowed":    true,
	"StatusOK":                  true,
	"StatusServiceUnavailable":  true,
	"StatusTooManyRequests":     true,
	"StatusUnauthorized":        true,
	"StatusUnprocessableEntity": true,
	"StatusUpgradeRequired":     true,
}

var allowedCoreSQLSelectors = map[string]bool{
	"DB":          true,
	"ErrConnDone": true,
	"ErrNoRows":   true,
	"ErrTxDone":   true,
}

var outboundMethodNames = map[string]bool{
	"Call":        true,
	"Dial":        true,
	"DialContext": true,
	"Do":          true,
	"Get":         true,
	"Head":        true,
	"Invoke":      true,
	"NewStream":   true,
	"Post":        true,
	"PostForm":    true,
	"RoundTrip":   true,
}

type coreSyncDeclaration struct {
	name       string
	sourceName string
	node       ast.Node
	imports    map[string]string
	dotImports []string
}

func TestCoreSyncBoundaryAudit(t *testing.T) {
	sources := loadCoreSyncSources(t)
	if err := auditCoreSyncBoundary(sources); err != nil {
		t.Fatalf("core sync dependency audit: %v", err)
	}

	t.Run("rejects outbound HTTP", func(t *testing.T) {
		mutated := maps.Clone(sources)
		const signature = "func (h *Handler) serveConnect(w http.ResponseWriter, r *http.Request) {\n"
		const injected = signature + "\tcoreSyncBoundaryOutboundHTTP()\n"
		handlers := mutated["api/go/handlers.go"]
		if strings.Count(string(handlers), signature) != 1 {
			t.Fatal("serveConnect source signature changed")
		}
		handlers = []byte(strings.Replace(string(handlers), signature, injected, 1))
		handlers = append(handlers, []byte(`
func coreSyncBoundaryOutboundHTTP() {
	_, _ = http.Get("http://127.0.0.1")
}
`)...)
		mutated["api/go/handlers.go"] = handlers
		err := auditCoreSyncBoundary(mutated)
		if err == nil || !strings.Contains(err.Error(), "net/http.Get") {
			t.Fatalf("outbound HTTP negative control passed: %v", err)
		}
	})
}

func assertCoreSyncBoundary(t *testing.T) {
	t.Helper()
	if err := auditCoreSyncBoundary(loadCoreSyncSources(t)); err != nil {
		t.Fatalf("SYNC-PERFORMANCE-001 core sync dependency audit: %v", err)
	}
}

func loadCoreSyncSources(t *testing.T) map[string][]byte {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve core sync audit source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	apiDir := filepath.Join(root, "api", "go")
	entries, err := os.ReadDir(apiDir)
	if err != nil {
		t.Fatalf("read adapter source directory: %v", err)
	}
	sources := make(map[string][]byte)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		name := path.Join("api/go", entry.Name())
		data, err := os.ReadFile(filepath.Join(apiDir, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sources[name] = data
	}
	return sources
}

func auditCoreSyncBoundary(sources map[string][]byte) error {
	fileSet := token.NewFileSet()
	declarations := make(map[string][]*coreSyncDeclaration)
	for sourceName, data := range sources {
		file, err := parser.ParseFile(fileSet, sourceName, data, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", sourceName, err)
		}
		imports, dotImports, err := coreSyncImports(file)
		if err != nil {
			return fmt.Errorf("%s: %w", sourceName, err)
		}
		for _, declaration := range file.Decls {
			switch value := declaration.(type) {
			case *ast.FuncDecl:
				record := &coreSyncDeclaration{
					name: value.Name.Name, sourceName: sourceName, node: value,
					imports: imports, dotImports: dotImports,
				}
				declarations[record.name] = append(declarations[record.name], record)
			case *ast.GenDecl:
				for _, spec := range value.Specs {
					var names []*ast.Ident
					switch value := spec.(type) {
					case *ast.TypeSpec:
						names = []*ast.Ident{value.Name}
					case *ast.ValueSpec:
						names = value.Names
					}
					for _, name := range names {
						record := &coreSyncDeclaration{
							name: name.Name, sourceName: sourceName, node: spec,
							imports: imports, dotImports: dotImports,
						}
						declarations[record.name] = append(declarations[record.name], record)
					}
				}
			}
		}
	}

	queue := make([]*coreSyncDeclaration, 0, len(coreSyncEntrypoints))
	for _, entrypoint := range coreSyncEntrypoints {
		candidates := declarations[entrypoint]
		if len(candidates) != 1 {
			return fmt.Errorf("core entrypoint %s has %d declarations", entrypoint, len(candidates))
		}
		queue = append(queue, candidates[0])
	}
	visited := make(map[*coreSyncDeclaration]bool)
	for len(queue) > 0 {
		declaration := queue[0]
		queue = queue[1:]
		if visited[declaration] {
			continue
		}
		visited[declaration] = true
		if len(declaration.dotImports) != 0 {
			return fmt.Errorf("%s declaration %s uses dot imports", declaration.sourceName, declaration.name)
		}
		if err := auditCoreSyncDeclaration(fileSet, declaration, declarations, &queue); err != nil {
			return err
		}
	}
	return nil
}

func coreSyncImports(file *ast.File) (map[string]string, []string, error) {
	imports := make(map[string]string)
	var dotImports []string
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid import path %s", spec.Path.Value)
		}
		name := path.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		if name == "." {
			dotImports = append(dotImports, importPath)
		} else if name != "_" {
			imports[name] = importPath
		}
	}
	return imports, dotImports, nil
}

func auditCoreSyncDeclaration(
	fileSet *token.FileSet,
	declaration *coreSyncDeclaration,
	declarations map[string][]*coreSyncDeclaration,
	queue *[]*coreSyncDeclaration,
) error {
	var violation error
	if typeSpec, ok := declaration.node.(*ast.TypeSpec); ok {
		ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
			if _, ok := node.(*ast.FuncType); ok {
				violation = coreSyncViolation(
					fileSet, declaration, node.Pos(),
					"function-typed dependency cannot prove the core boundary",
				)
				return false
			}
			return true
		})
	}
	ast.Inspect(declaration.node, func(node ast.Node) bool {
		if node == nil || violation != nil {
			return false
		}
		if identifier, ok := node.(*ast.Ident); ok {
			*queue = append(*queue, declarations[identifier.Name]...)
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && outboundMethodNames[selector.Sel.Name] {
				packageName, packageSelector := selector.X.(*ast.Ident)
				if !packageSelector || packageName.Obj != nil || declaration.imports[packageName.Name] == "" {
					violation = coreSyncViolation(fileSet, declaration, selector.Pos(), "outbound method "+selector.Sel.Name)
					return false
				}
			}
		}
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		*queue = append(*queue, declarations[selector.Sel.Name]...)
		packageName, ok := selector.X.(*ast.Ident)
		if !ok || packageName.Obj != nil {
			return true
		}
		importPath, ok := declaration.imports[packageName.Name]
		if !ok {
			return true
		}
		if reason := forbiddenCorePackageSelector(importPath, selector.Sel.Name); reason != "" {
			violation = coreSyncViolation(fileSet, declaration, selector.Pos(), reason)
			return false
		}
		return true
	})
	return violation
}

func forbiddenCorePackageSelector(importPath, selector string) string {
	if importPath == "database/sql" {
		if allowedCoreSQLSelectors[selector] {
			return ""
		}
		return "forbidden additional database capability database/sql." + selector
	}
	if importPath == "database/sql/driver" {
		if selector == "ErrBadConn" {
			return ""
		}
		return "forbidden additional database capability database/sql/driver." + selector
	}
	if importPath == "net/http" {
		if allowedCoreHTTPSelectors[selector] {
			return ""
		}
		return "forbidden outbound dependency net/http." + selector
	}
	if importPath == "net" {
		if selector == "Error" {
			return ""
		}
		return "forbidden network dependency net." + selector
	}
	if strings.HasPrefix(importPath, "net/") || importPath == "crypto/tls" ||
		importPath == "os/exec" || importPath == "syscall" {
		return "forbidden network-capable dependency " + importPath + "." + selector
	}
	if strings.Contains(strings.Split(importPath, "/")[0], ".") {
		return "forbidden external dependency " + importPath + "." + selector
	}
	return ""
}

func coreSyncViolation(
	fileSet *token.FileSet,
	declaration *coreSyncDeclaration,
	position token.Pos,
	reason string,
) error {
	location := fileSet.Position(position)
	return fmt.Errorf("%s:%d declaration %s: %s", declaration.sourceName, location.Line, declaration.name, reason)
}
