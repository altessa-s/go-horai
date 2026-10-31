// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"golang.org/x/mod/modfile"
)

const module = "github.com/altessa-s/go-horai"
const core = "github.com/altessa-s/go-atlas-core/"

// Every module is walked. devtools has a separate tool-import allowlist.
func layers() map[string][]string {
	return map[string][]string{
		".":    {module + "/task", module + "/query"},
		"task": nil, "query": nil, "schedule": {"github.com/robfig/cron/v3"},
		"scheduler":                      {module, module + "/task", module + "/query", module + "/schedule", core + "context", core + "errors", core + "collections/...", core + "runtime/concurrency", core + "time/clock", core + "types/nilcheck"},
		"storage/memory":                 {module, module + "/query"},
		"storage/sqldb":                  {module, module + "/task", module + "/query", module + "/storage/sqldb/internal/dialect", core + "..."},
		"storage/sqldb/internal/dialect": nil,
		"storage/redis":                  {module, module + "/task", module + "/query", core + "...", "github.com/redis/go-redis/v9/..."},
		"storage/mongo":                  {module, module + "/task", module + "/query", core + "...", "go.mongodb.org/mongo-driver/v2/..."},
		"storage/clickhouse":             {module, module + "/query", core + "...", "github.com/ClickHouse/clickhouse-go/v2/..."},
		"horaitest":                      {module, module + "/task", core + "time/clock", "github.com/stretchr/testify/..."},
		"storage/storagetest":            {module, module + "/task", module + "/query", "github.com/stretchr/testify/..."},
		"internal/testhelpers":           {module + "/query"},
		"integration/docker":             nil,
		"devtools":                       {"github.com/altessa-s/go-copyright-checker/...", "github.com/altessa-s/go-optgen/...", "github.com/daixiang0/gci", "github.com/golangci/golangci-lint/v2/...", "github.com/mibk/dupl", "github.com/securego/gosec/v2/...", "golang.org/x/perf/...", "golang.org/x/vuln/..."},
	}
}
func matches(pattern, imp string) bool {
	if p, ok := strings.CutSuffix(pattern, "/..."); ok {
		return imp == p || strings.HasPrefix(imp, p+"/")
	}
	return pattern == imp
}
func stdlib(imp string) bool {
	first, _, _ := strings.Cut(imp, "/")
	return !strings.Contains(first, ".")
}
func TestArchitecture(t *testing.T) {
	t.Parallel()
	table := layers()
	seen := map[string]bool{}
	modules := map[string]bool{}
	err := filepath.WalkDir(".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != "." && (strings.HasPrefix(d.Name(), ".") || d.Name() == "bin" || d.Name() == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		dir := filepath.ToSlash(filepath.Dir(p))
		if d.Name() == "go.mod" {
			modules[dir] = true
			checkMod(t, p, dir)
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.ParseComments)
		require.NoError(t, err, p)
		allow, ok := table[dir]
		require.True(t, ok, "add package %s to the layers table", dir)
		seen[dir] = true
		test := strings.HasSuffix(p, "_test.go")
		if test && !strings.HasSuffix(f.Name.Name, "_test") {
			require.True(t, strings.HasSuffix(p, "_internal_test.go"), "white-box test must be named *_internal_test.go: %s", p)
		}
		generated := ast.IsGenerated(f)
		for _, i := range f.Imports {
			imp, err := strconv.Unquote(i.Path.Value)
			require.NoError(t, err)
			require.False(t, imp == "github.com/altessa-s/go-atlas" || strings.HasPrefix(imp, "github.com/altessa-s/go-atlas/"), "monolith import in %s", p)
			if strings.HasPrefix(imp, "github.com/altessa-s/") && dir != "devtools" {
				require.True(t, imp == module || strings.HasPrefix(imp, module+"/") || strings.HasPrefix(imp, core) || (dir == "integration/docker" && (imp == "github.com/altessa-s/go-tlsutils" || strings.HasPrefix(imp, "github.com/altessa-s/go-tlsutils/"))), "unapproved sibling %s in %s", imp, p)
			}
			require.NotEqual(t, "github.com/stretchr/testify/"+"assert", imp, "use require: %s", p)
			if test {
				continue
			}
			kit := dir == "horaitest" || dir == "storage/storagetest" || dir == "internal/testhelpers"
			require.False(t, !kit && (imp == "testing" || strings.HasPrefix(imp, "github.com/stretchr/testify") || strings.HasSuffix(imp, "/horaitest") || strings.HasSuffix(imp, "/storagetest") || strings.HasSuffix(imp, "/testhelpers")), "test kit used in production: %s", p)
			if !stdlib(imp) {
				require.True(t, slices.ContainsFunc(allow, func(a string) bool { return matches(a, imp) }), "forbidden dependency %s in %s", imp, p)
			}
		}

		if d.Name() == "options.go" {
			generated, err := os.ReadFile(filepath.Join(filepath.Dir(p), "options_gen.go"))
			require.NoError(t, err, "options require generated companion: %s", p)
			require.Contains(t, string(generated), "Code generated", p)
		}
		for _, decl := range f.Decls {
			switch x := decl.(type) {
			case *ast.FuncDecl:
				require.NotEqual(t, "init", x.Name.Name, "init in %s", p)
				if strings.HasPrefix(x.Name.Name, "With") {
					require.True(t, generated, "hand-written With function or method in %s", p)
				}
			case *ast.GenDecl:
				if !test && x.Tok == token.VAR {
					for _, spec := range x.Specs {
						v := spec.(*ast.ValueSpec)
						for i, n := range v.Names {
							if !ast.IsExported(n.Name) {
								continue
							}
							require.Regexp(t, `^Err[A-Z]`, n.Name, "exported mutable var in %s", p)
							require.Greater(t, len(v.Values), i, "sentinel initializer in %s", p)
							call, ok := v.Values[i].(*ast.CallExpr)
							require.True(t, ok, p)
							selector, ok := call.Fun.(*ast.SelectorExpr)
							require.True(t, ok, p)
							pkg, ok := selector.X.(*ast.Ident)
							require.True(t, ok, p)
							require.True(t, (pkg.Name == "errors" && selector.Sel.Name == "New") || (pkg.Name == "fmt" && selector.Sel.Name == "Errorf"), "sentinel initializer in %s", p)
						}

					}
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
	for dir := range seen {
		for _, f := range []string{"doc.go", "README.md"} {
			_, err := os.Stat(path.Join(dir, f))
			require.NoError(t, err, "package %s needs %s", dir, f)
		}
	}
	for _, dir := range []string{".", "storage/redis", "storage/mongo", "storage/clickhouse", "integration/docker", "devtools"} {
		require.True(t, modules[dir], "module %s was not walked", dir)
	}
}
func checkMod(t *testing.T, p, dir string) {
	t.Helper()
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	direct := map[string][]string{
		".":                  {"github.com/altessa-s/go-atlas-core", "github.com/robfig/cron/v3", "github.com/stretchr/testify", "golang.org/x/mod"},
		"storage/redis":      {module, "github.com/altessa-s/go-atlas-core", "github.com/stretchr/testify", "github.com/redis/go-redis/v9"},
		"storage/mongo":      {module, "github.com/altessa-s/go-atlas-core", "github.com/stretchr/testify", "go.mongodb.org/mongo-driver/v2"},
		"storage/clickhouse": {module, "github.com/altessa-s/go-atlas-core", "github.com/stretchr/testify", "github.com/ClickHouse/clickhouse-go/v2"},
	}
	parsed, err := modfile.Parse(p, b, nil)
	require.NoError(t, err)
	allowedReplacements := map[string]string{}
	if strings.HasPrefix(dir, "storage/") || dir == "integration/docker" {
		allowedReplacements[module] = "../.."
	}
	if dir == "integration/docker" {
		for _, store := range []string{"redis", "mongo", "clickhouse"} {
			allowedReplacements[module+"/storage/"+store] = "../../storage/" + store
		}
	}
	require.Len(t, parsed.Replace, len(allowedReplacements), "exact local module replacements in %s", p)
	for _, r := range parsed.Replace {
		want, ok := allowedReplacements[r.Old.Path]
		require.True(t, ok, "external replacement in %s", p)
		require.Equal(t, want, r.New.Path, p)
		require.Empty(t, r.Old.Version, p)
		require.Empty(t, r.New.Version, p)
	}
	for _, r := range parsed.Require {
		require.False(t, regexp.MustCompile(`^github\.com/altessa-s/go-atlas(?:/|$)`).MatchString(r.Mod.Path), "monolith in %s", p)
		if strings.Contains(r.Mod.Path, "testcontainers") {
			require.True(t, dir == "integration/docker" || dir == "devtools")
		}
		if allow, ok := direct[dir]; ok && !r.Indirect {
			require.Contains(t, allow, r.Mod.Path, "direct dependency purity in %s", p)
		}
	}
}
