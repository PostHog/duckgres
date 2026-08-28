package core

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	identifierPartRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type relationPlaceholder struct {
	start int
	end   int
	role  string
}

type catalogFile struct {
	Name              string                              `yaml:"name"`
	Description       string                              `yaml:"description"`
	Seed              int64                               `yaml:"seed"`
	DatasetScale      int                                 `yaml:"dataset_scale"`
	Targets           []Protocol                          `yaml:"targets"`
	WarmupIterations  int                                 `yaml:"warmup_iterations"`
	MeasureIterations int                                 `yaml:"measure_iterations"`
	RelationVariants  map[StorageTarget]map[string]string `yaml:"relation_variants"`
}

type pairedQueryDefinition struct {
	QueryIDBase string         `yaml:"query_id_base"`
	IntentID    string         `yaml:"intent_id"`
	Tags        []string       `yaml:"tags"`
	Params      map[string]any `yaml:"params"`
	SQLTemplate string         `yaml:"sql_template"`
}

type catalogEntry struct {
	legacy *Query
	paired *pairedQueryDefinition
}

func LoadCatalog(path string) (Catalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Catalog{}, fmt.Errorf("read catalog %s: %w", path, err)
	}
	return ParseCatalog(b)
}

func ParseCatalog(raw []byte) (Catalog, error) {
	var file catalogFile
	if err := yaml.Unmarshal(raw, &file); err != nil {
		return Catalog{}, fmt.Errorf("parse catalog: %w", err)
	}
	c := Catalog{
		Name:              file.Name,
		Description:       file.Description,
		Seed:              file.Seed,
		DatasetScale:      file.DatasetScale,
		Targets:           file.Targets,
		WarmupIterations:  file.WarmupIterations,
		MeasureIterations: file.MeasureIterations,
	}
	entries, err := catalogEntries(raw)
	if err != nil {
		return Catalog{}, err
	}
	if len(entries) > 0 {
		if err := validateRelationVariants(file.RelationVariants, entries); err != nil {
			return Catalog{}, err
		}
	}
	for _, entry := range entries {
		switch {
		case entry.legacy != nil:
			c.Queries = append(c.Queries, *entry.legacy)
		case entry.paired != nil:
			queries, err := expandPairedQuery(*entry.paired, file.RelationVariants)
			if err != nil {
				return Catalog{}, err
			}
			c.Queries = append(c.Queries, queries...)
		}
	}
	if err := validateCatalog(c); err != nil {
		return Catalog{}, err
	}
	return c, nil
}

// catalogEntries preserves the declaration order of legacy and paired lists
// when they are mixed in a YAML mapping. The runtime still receives only the
// expanded Catalog.Queries slice.
func catalogEntries(raw []byte) ([]catalogEntry, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("parse catalog: %w", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("parse catalog: expected a mapping")
	}
	mapping := document.Content[0]
	var entries []catalogEntry
	for index := 0; index < len(mapping.Content); index += 2 {
		key, value := mapping.Content[index], mapping.Content[index+1]
		switch key.Value {
		case "queries":
			var queries []Query
			if err := value.Decode(&queries); err != nil {
				return nil, fmt.Errorf("parse legacy queries: %w", err)
			}
			for i := range queries {
				entries = append(entries, catalogEntry{legacy: &queries[i]})
			}
		case "paired_queries":
			var paired []pairedQueryDefinition
			if err := value.Decode(&paired); err != nil {
				return nil, fmt.Errorf("parse paired queries: %w", err)
			}
			for i := range paired {
				entries = append(entries, catalogEntry{paired: &paired[i]})
			}
		}
	}
	return entries, nil
}

func validateRelationVariants(variants map[StorageTarget]map[string]string, entries []catalogEntry) error {
	hasPairedQueries := false
	for _, entry := range entries {
		if entry.paired != nil {
			hasPairedQueries = true
			break
		}
	}
	if !hasPairedQueries {
		return nil
	}
	if len(variants) != 2 {
		return fmt.Errorf("paired catalogs must declare exactly the raw_view and ducklake_table storage variants")
	}
	for _, target := range []StorageTarget{StorageTargetRawView, StorageTargetDuckLakeTable} {
		if _, ok := variants[target]; !ok {
			return fmt.Errorf("paired catalogs must declare exactly the raw_view and ducklake_table storage variants")
		}
	}
	return nil
}

func expandPairedQuery(def pairedQueryDefinition, variants map[StorageTarget]map[string]string) ([]Query, error) {
	if def.QueryIDBase == "" {
		return nil, fmt.Errorf("paired query missing query_id_base")
	}
	if def.IntentID == "" {
		return nil, fmt.Errorf("paired query %s missing intent_id", def.QueryIDBase)
	}
	placeholders, err := scanRelationPlaceholders(def.QueryIDBase, def.SQLTemplate)
	if err != nil {
		return nil, err
	}
	if len(placeholders) == 0 {
		return nil, fmt.Errorf("paired query %s must contain at least one relation placeholder", def.QueryIDBase)
	}

	queries := make([]Query, 0, 2)
	for _, target := range []StorageTarget{StorageTargetRawView, StorageTargetDuckLakeTable} {
		rendered, err := renderRelationTemplate(def.QueryIDBase, def.SQLTemplate, placeholders, variants[target], target)
		if err != nil {
			return nil, err
		}
		queryID := def.QueryIDBase + "__" + string(target)
		if err := validateSelectOnlySQL("sql_template", queryID, rendered); err != nil {
			return nil, err
		}
		queries = append(queries, Query{
			QueryID:       queryID,
			IntentID:      def.IntentID,
			Tags:          def.Tags,
			Params:        def.Params,
			PGWireSQL:     rendered,
			StorageTarget: target,
		})
	}
	if queries[0].PGWireSQL == queries[1].PGWireSQL {
		return nil, fmt.Errorf("paired query %s relation bindings must differ between storage targets", def.QueryIDBase)
	}
	return queries, nil
}

func scanRelationPlaceholders(queryID, sql string) ([]relationPlaceholder, error) {
	var placeholders []relationPlaceholder
	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			end, closed := skipSingleQuotedSQLString(sql, i)
			if !closed {
				return nil, fmt.Errorf("paired query %s has an unterminated SQL string", queryID)
			}
			if containsTemplateDelimiter(sql[i:end]) {
				return nil, fmt.Errorf("paired query %s relation placeholder must appear in SQL code, not a string", queryID)
			}
			i = end
		case sql[i] == '"':
			end, closed := skipDoubleQuotedSQLIdentifier(sql, i)
			if !closed {
				return nil, fmt.Errorf("paired query %s has an unterminated quoted identifier", queryID)
			}
			if containsTemplateDelimiter(sql[i:end]) {
				return nil, fmt.Errorf("paired query %s relation placeholder must appear in SQL code, not a quoted identifier", queryID)
			}
			i = end
		case i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-':
			end := skipSQLLineComment(sql, i)
			if containsTemplateDelimiter(sql[i:end]) {
				return nil, fmt.Errorf("paired query %s relation placeholder must appear in SQL code, not a comment", queryID)
			}
			i = end
		case i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*':
			end, closed := skipSQLBlockComment(sql, i)
			if !closed {
				return nil, fmt.Errorf("paired query %s has an unterminated SQL comment", queryID)
			}
			if containsTemplateDelimiter(sql[i:end]) {
				return nil, fmt.Errorf("paired query %s relation placeholder must appear in SQL code, not a comment", queryID)
			}
			i = end
		case sql[i] == '$':
			delimiter, ok := dollarQuoteDelimiterAt(sql, i)
			if !ok {
				i++
				continue
			}
			end, closed := skipDollarQuotedSQLString(sql, i, delimiter)
			if !closed {
				return nil, fmt.Errorf("paired query %s has an unterminated dollar-quoted string", queryID)
			}
			if containsTemplateDelimiter(sql[i:end]) {
				return nil, fmt.Errorf("paired query %s relation placeholder must appear in SQL code, not a string", queryID)
			}
			i = end
		case strings.HasPrefix(sql[i:], "{{"):
			placeholder, err := parseRelationPlaceholder(sql, i)
			if err != nil {
				return nil, fmt.Errorf("paired query %s has unsupported template action: %w", queryID, err)
			}
			placeholders = append(placeholders, placeholder)
			i = placeholder.end
		case strings.HasPrefix(sql[i:], "}}"):
			return nil, fmt.Errorf("paired query %s has unsupported template action: unmatched closing braces", queryID)
		default:
			i++
		}
	}
	return placeholders, nil
}

func parseRelationPlaceholder(sql string, start int) (relationPlaceholder, error) {
	i := start + 2
	i = skipTemplateWhitespace(sql, i)
	if !strings.HasPrefix(sql[i:], "relation") {
		return relationPlaceholder{}, fmt.Errorf("expected relation action")
	}
	i += len("relation")
	if i >= len(sql) || !isTemplateWhitespace(sql[i]) {
		return relationPlaceholder{}, fmt.Errorf("expected whitespace after relation")
	}
	i = skipTemplateWhitespace(sql, i)
	if i >= len(sql) || sql[i] != '"' {
		return relationPlaceholder{}, fmt.Errorf("expected a quoted relation role")
	}
	i++
	roleStart := i
	for i < len(sql) && isSQLRoleIdentifierPart(sql[i]) {
		i++
	}
	role := sql[roleStart:i]
	if role == "" || !identifierPartRE.MatchString(role) {
		return relationPlaceholder{}, fmt.Errorf("invalid relation role")
	}
	if i >= len(sql) || sql[i] != '"' {
		return relationPlaceholder{}, fmt.Errorf("expected closing quote after relation role")
	}
	i++
	i = skipTemplateWhitespace(sql, i)
	if i+1 >= len(sql) || sql[i] != '}' || sql[i+1] != '}' {
		return relationPlaceholder{}, fmt.Errorf("expected closing braces")
	}
	end := i + 2
	if end < len(sql) && sql[end] == '}' {
		return relationPlaceholder{}, fmt.Errorf("unexpected extra closing brace")
	}
	return relationPlaceholder{start: start, end: end, role: role}, nil
}

func renderRelationTemplate(queryID, template string, placeholders []relationPlaceholder, bindings map[string]string, target StorageTarget) (string, error) {
	var rendered strings.Builder
	last := 0
	for _, placeholder := range placeholders {
		binding, ok := bindings[placeholder.role]
		if !ok || binding == "" {
			return "", fmt.Errorf("paired query %s missing relation binding for role %q in storage target %q", queryID, placeholder.role, target)
		}
		quoted, err := quoteRelationIdentifier(binding)
		if err != nil {
			return "", fmt.Errorf("paired query %s has invalid relation identifier for role %q in storage target %q: %w", queryID, placeholder.role, target, err)
		}
		rendered.WriteString(template[last:placeholder.start])
		rendered.WriteString(quoted)
		last = placeholder.end
	}
	rendered.WriteString(template[last:])
	return rendered.String(), nil
}

func skipTemplateWhitespace(sql string, start int) int {
	for start < len(sql) && isTemplateWhitespace(sql[start]) {
		start++
	}
	return start
}

func isTemplateWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v'
}

func isSQLRoleIdentifierPart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')
}

func containsTemplateDelimiter(sql string) bool {
	return strings.Contains(sql, "{{") || strings.Contains(sql, "}}")
}

func quoteRelationIdentifier(identifier string) (string, error) {
	parts := strings.Split(identifier, ".")
	if len(parts) == 0 {
		return "", fmt.Errorf("empty identifier")
	}
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		if !identifierPartRE.MatchString(part) {
			return "", fmt.Errorf("%q is not a dot-separated identifier", identifier)
		}
		quoted = append(quoted, `"`+part+`"`)
	}
	return strings.Join(quoted, "."), nil
}

func validateCatalog(c Catalog) error {
	if c.Name == "" {
		return fmt.Errorf("catalog name is required")
	}
	if c.MeasureIterations <= 0 {
		return fmt.Errorf("measure_iterations must be > 0")
	}
	if c.DatasetScale <= 0 {
		return fmt.Errorf("dataset_scale must be > 0")
	}
	if len(c.Targets) == 0 {
		return fmt.Errorf("targets must include at least one protocol")
	}
	seenTargets := map[Protocol]struct{}{}
	for _, target := range c.Targets {
		if target != ProtocolPGWire {
			return fmt.Errorf("unsupported target protocol %q", target)
		}
		if _, ok := seenTargets[target]; ok {
			return fmt.Errorf("duplicate target protocol %q", target)
		}
		seenTargets[target] = struct{}{}
	}
	if len(c.Queries) == 0 {
		return fmt.Errorf("queries must include at least one entry")
	}
	seenQueryIDs := map[string]struct{}{}
	hasPairedQueries := false
	for _, q := range c.Queries {
		if q.QueryID == "" {
			return fmt.Errorf("query_id is required")
		}
		if _, ok := seenQueryIDs[q.QueryID]; ok {
			return fmt.Errorf("duplicate query_id %q", q.QueryID)
		}
		seenQueryIDs[q.QueryID] = struct{}{}
		if q.IntentID == "" {
			return fmt.Errorf("query %s missing intent_id", q.QueryID)
		}
		if q.PGWireSQL == "" {
			return fmt.Errorf("query %s missing pgwire_sql", q.QueryID)
		}
		if q.StorageTarget != "" {
			hasPairedQueries = true
		}
	}
	if hasPairedQueries && c.MeasureIterations%2 != 0 {
		return fmt.Errorf("paired catalogs require an even measure_iterations value to balance storage-target execution order")
	}
	return nil
}

func ValidateReadOnlyCatalog(c Catalog) error {
	for _, q := range c.Queries {
		if err := validateSelectOnlySQL("pgwire_sql", q.QueryID, q.PGWireSQL); err != nil {
			return err
		}
	}
	return nil
}

func validateSelectOnlySQL(field, queryID, sql string) error {
	trimmed := trimLeadingSQLComments(sql)
	trimmed = strings.TrimSpace(trimmed)
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return fmt.Errorf("query %s missing %s", queryID, field)
	}
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") {
		return fmt.Errorf("query %s %s must be SELECT-only in frozen mode", queryID, field)
	}
	if len(upper) > len("SELECT") {
		next := upper[len("SELECT")]
		if (next >= 'A' && next <= 'Z') || (next >= '0' && next <= '9') || next == '_' {
			return fmt.Errorf("query %s %s must be SELECT-only in frozen mode", queryID, field)
		}
	}
	if strings.Contains(trimmed, ";") {
		return fmt.Errorf("query %s %s must contain a single SELECT statement in frozen mode", queryID, field)
	}
	if containsSQLKeyword(trimmed, "INTO") {
		return fmt.Errorf("query %s %s must be SELECT-only in frozen mode", queryID, field)
	}
	return nil
}

func containsSQLKeyword(sql, keyword string) bool {
	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			i, _ = skipSingleQuotedSQLString(sql, i)
		case sql[i] == '"':
			i, _ = skipDoubleQuotedSQLIdentifier(sql, i)
		case i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-':
			i = skipSQLLineComment(sql, i)
		case i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*':
			i, _ = skipSQLBlockComment(sql, i)
		case sql[i] == '$':
			delimiter, ok := dollarQuoteDelimiterAt(sql, i)
			if ok {
				i, _ = skipDollarQuotedSQLString(sql, i, delimiter)
			} else {
				i++
			}
		case isSQLIdentifierStart(sql[i]):
			start := i
			i++
			for i < len(sql) && isSQLIdentifierPart(sql[i]) {
				i++
			}
			if strings.EqualFold(sql[start:i], keyword) {
				return true
			}
		default:
			i++
		}
	}
	return false
}

func skipSingleQuotedSQLString(sql string, start int) (int, bool) {
	for i := start + 1; i < len(sql); i++ {
		switch sql[i] {
		case '\\':
			if i+1 < len(sql) {
				i++
			}
		case '\'':
			if i+1 < len(sql) && sql[i+1] == '\'' {
				i++
				continue
			}
			return i + 1, true
		}
	}
	return len(sql), false
}

func skipDoubleQuotedSQLIdentifier(sql string, start int) (int, bool) {
	for i := start + 1; i < len(sql); i++ {
		if sql[i] != '"' {
			continue
		}
		if i+1 < len(sql) && sql[i+1] == '"' {
			i++
			continue
		}
		return i + 1, true
	}
	return len(sql), false
}

func skipSQLLineComment(sql string, start int) int {
	i := start + 2
	for i < len(sql) && sql[i] != '\n' {
		i++
	}
	return i
}

func skipSQLBlockComment(sql string, start int) (int, bool) {
	depth := 1
	for i := start + 2; i < len(sql); {
		switch {
		case i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*':
			depth++
			i += 2
		case i+1 < len(sql) && sql[i] == '*' && sql[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i, true
			}
		default:
			i++
		}
	}
	return len(sql), false
}

func dollarQuoteDelimiterAt(sql string, start int) (string, bool) {
	if start >= len(sql) || sql[start] != '$' || start+1 >= len(sql) {
		return "", false
	}
	if sql[start+1] == '$' {
		return "$$", true
	}
	if !isSQLIdentifierStart(sql[start+1]) {
		return "", false
	}
	i := start + 2
	for i < len(sql) && isSQLRoleIdentifierPart(sql[i]) {
		i++
	}
	if i >= len(sql) || sql[i] != '$' {
		return "", false
	}
	return sql[start : i+1], true
}

func skipDollarQuotedSQLString(sql string, start int, delimiter string) (int, bool) {
	bodyStart := start + len(delimiter)
	offset := strings.Index(sql[bodyStart:], delimiter)
	if offset < 0 {
		return len(sql), false
	}
	return bodyStart + offset + len(delimiter), true
}

func isSQLIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isSQLIdentifierPart(ch byte) bool {
	return isSQLIdentifierStart(ch) || (ch >= '0' && ch <= '9') || ch == '$'
}

func trimLeadingSQLComments(sql string) string {
	remaining := strings.TrimSpace(sql)
	for {
		switch {
		case strings.HasPrefix(remaining, "--"):
			idx := strings.IndexByte(remaining, '\n')
			if idx < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[idx+1:])
		case strings.HasPrefix(remaining, "/*"):
			idx := strings.Index(remaining, "*/")
			if idx < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[idx+2:])
		default:
			return remaining
		}
	}
}
