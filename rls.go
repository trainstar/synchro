package synchro

import "fmt"

// GenerateRLSPolicies generates SQL statements to enable RLS and create
// policies for all registered tables based on registry configuration.
//
// ownerColumn is the column name that the BucketFunc uses for user ownership
// (typically "user_id"). For tables that have this column, owner-based policies
// are generated. For child tables, EXISTS through the FK chain. For tables
// without the column and no FK chain, read-all reference policies.
//
// Column nullability (introspected) determines whether NULL-owner rows are
// readable by all users (global read).
func GenerateRLSPolicies(registry *Registry, ownerColumn string) []string {
	var stmts []string

	for _, cfg := range registry.All() {
		stmts = append(stmts,
			fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", quoteIdentifier(cfg.TableName)),
			fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", quoteIdentifier(cfg.TableName)),
		)

		hasOwnerCol := cfg.columnNullable != nil && hasColumn(cfg, ownerColumn)
		hasParent := cfg.parentTable != ""

		// No ownership column and no parent: read-all reference table.
		if !hasOwnerCol && !hasParent {
			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (true)`,
					quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName)),
			)
			continue
		}

		if hasOwnerCol {
			// Use column nullability to determine if global reads are allowed.
			isNullable := cfg.ColumnNullable(ownerColumn)

			readPredicate := fmt.Sprintf("%s::text = current_setting('app.user_id', true)",
				quoteIdentifier(ownerColumn))
			if isNullable {
				readPredicate = fmt.Sprintf("(%s IS NULL OR %s)",
					quoteIdentifier(ownerColumn), readPredicate)
			}
			writePredicate := fmt.Sprintf("%s::text = current_setting('app.user_id', true)",
				quoteIdentifier(ownerColumn))

			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (%s)`,
					quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName), readPredicate),
			)
			// Generate write policies for all tables with an owner column.
			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR INSERT WITH CHECK (%s)`,
					quoteIdentifier("sync_write_ins_"+cfg.TableName), quoteIdentifier(cfg.TableName), writePredicate),
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR UPDATE USING (%s)`,
					quoteIdentifier("sync_write_upd_"+cfg.TableName), quoteIdentifier(cfg.TableName), writePredicate),
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR DELETE USING (%s)`,
					quoteIdentifier("sync_write_del_"+cfg.TableName), quoteIdentifier(cfg.TableName), writePredicate),
			)
			continue
		}

		// Child table: build EXISTS through introspected FK chain.
		readCheck := buildParentOwnerCheck(registry, cfg, ownerColumn)
		if readCheck != "" {
			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (%s)`,
					quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName), readCheck),
			)
			// Write policies for child tables.
			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR INSERT WITH CHECK (%s)`,
					quoteIdentifier("sync_write_ins_"+cfg.TableName), quoteIdentifier(cfg.TableName), readCheck),
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR UPDATE USING (%s)`,
					quoteIdentifier("sync_write_upd_"+cfg.TableName), quoteIdentifier(cfg.TableName), readCheck),
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR DELETE USING (%s)`,
					quoteIdentifier("sync_write_del_"+cfg.TableName), quoteIdentifier(cfg.TableName), readCheck),
			)
		}
	}

	return stmts
}

// hasColumn returns true if the column exists in the table's introspected metadata.
func hasColumn(cfg *TableConfig, col string) bool {
	_, ok := cfg.columnNullable[col]
	return ok
}

// buildParentOwnerCheck builds nested EXISTS clauses that follow parent FKs to
// the chain root owner column.
func buildParentOwnerCheck(registry *Registry, cfg *TableConfig, ownerColumn string) string {
	return buildParentOwnerCheckAt(registry, cfg, quoteIdentifier(cfg.TableName), 1, ownerColumn)
}

func buildParentOwnerCheckAt(registry *Registry, cfg *TableConfig, childRef string, depth int, ownerColumn string) string {
	if cfg.parentTable == "" {
		return ""
	}

	parentCfg := registry.Get(cfg.parentTable)
	if parentCfg == nil {
		return ""
	}

	alias := fmt.Sprintf("p%d", depth)
	if hasColumn(parentCfg, ownerColumn) {
		isNullable := parentCfg.ColumnNullable(ownerColumn)

		var ownerPredicate string
		if isNullable {
			ownerPredicate = fmt.Sprintf("(%s.%s IS NULL OR %s.%s::text = current_setting('app.user_id', true))",
				alias, quoteIdentifier(ownerColumn),
				alias, quoteIdentifier(ownerColumn))
		} else {
			ownerPredicate = fmt.Sprintf("%s.%s::text = current_setting('app.user_id', true)",
				alias, quoteIdentifier(ownerColumn))
		}
		return fmt.Sprintf(`EXISTS (SELECT 1 FROM %s %s WHERE %s.%s = %s.%s AND %s)`,
			quoteIdentifier(parentCfg.TableName), alias,
			alias, quoteIdentifier(parentCfg.IDColumn),
			childRef, quoteIdentifier(cfg.parentFKCol),
			ownerPredicate)
	}

	inner := buildParentOwnerCheckAt(registry, parentCfg, alias, depth+1, ownerColumn)
	if inner == "" {
		return ""
	}
	return fmt.Sprintf(`EXISTS (SELECT 1 FROM %s %s WHERE %s.%s = %s.%s AND %s)`,
		quoteIdentifier(parentCfg.TableName), alias,
		alias, quoteIdentifier(parentCfg.IDColumn),
		childRef, quoteIdentifier(cfg.parentFKCol),
		inner)
}
