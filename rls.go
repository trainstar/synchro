package synchro

import "fmt"

// GenerateRLSPolicies generates SQL statements to enable RLS and create
// policies for all registered tables based on registry configuration.
//
// Ownership comparisons use text equality instead of hard-coded UUID casts so
// integrations can use non-UUID user identifiers.
func GenerateRLSPolicies(registry *Registry) []string {
	var stmts []string

	for _, cfg := range registry.All() {
		stmts = append(stmts,
			fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", quoteIdentifier(cfg.TableName)),
		)

		// No ownership columns anywhere: read-all table.
		if cfg.OwnerColumn == "" && cfg.ParentTable == "" {
			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (true)`,
					quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName)),
			)
			continue
		}

		if cfg.OwnerColumn != "" {
			readPredicate := fmt.Sprintf("%s::text = current_setting('app.user_id', true)",
				quoteIdentifier(cfg.OwnerColumn))
			if cfg.AllowGlobalRead {
				readPredicate = fmt.Sprintf("(%s IS NULL OR %s)",
					quoteIdentifier(cfg.OwnerColumn), readPredicate)
			}
			writePredicate := fmt.Sprintf("%s::text = current_setting('app.user_id', true)",
				quoteIdentifier(cfg.OwnerColumn))

			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (%s)`,
					quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName), readPredicate),
			)
			if registry.IsPushable(cfg.TableName) {
				stmts = append(stmts,
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR INSERT WITH CHECK (%s)`,
						quoteIdentifier("sync_write_ins_"+cfg.TableName), quoteIdentifier(cfg.TableName), writePredicate),
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR UPDATE USING (%s)`,
						quoteIdentifier("sync_write_upd_"+cfg.TableName), quoteIdentifier(cfg.TableName), writePredicate),
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR DELETE USING (%s)`,
						quoteIdentifier("sync_write_del_"+cfg.TableName), quoteIdentifier(cfg.TableName), writePredicate),
				)
			}
			continue
		}

		// Child table ownership checks are resolved through parent chains.
		readCheck := buildParentOwnerCheck(registry, cfg, cfg.AllowGlobalRead)
		if readCheck != "" {
			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (%s)`,
					quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName), readCheck),
			)
		}

		if registry.IsPushable(cfg.TableName) {
			writeCheck := buildParentOwnerCheck(registry, cfg, false)
			if writeCheck != "" {
				stmts = append(stmts,
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR INSERT WITH CHECK (%s)`,
						quoteIdentifier("sync_write_ins_"+cfg.TableName), quoteIdentifier(cfg.TableName), writeCheck),
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR UPDATE USING (%s)`,
						quoteIdentifier("sync_write_upd_"+cfg.TableName), quoteIdentifier(cfg.TableName), writeCheck),
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR DELETE USING (%s)`,
						quoteIdentifier("sync_write_del_"+cfg.TableName), quoteIdentifier(cfg.TableName), writeCheck),
				)
			}
		}
	}

	return stmts
}

// buildParentOwnerCheck builds nested EXISTS clauses that follow parent FKs to
// the chain root owner column.
func buildParentOwnerCheck(registry *Registry, cfg *TableConfig, includeGlobal bool) string {
	return buildParentOwnerCheckAt(registry, cfg, quoteIdentifier(cfg.TableName), 1, includeGlobal)
}

func buildParentOwnerCheckAt(registry *Registry, cfg *TableConfig, childRef string, depth int, includeGlobal bool) string {
	if cfg.ParentTable == "" {
		return ""
	}

	parentCfg := registry.Get(cfg.ParentTable)
	if parentCfg == nil {
		return ""
	}

	alias := fmt.Sprintf("p%d", depth)
	if parentCfg.OwnerColumn != "" {
		base := fmt.Sprintf(`EXISTS (SELECT 1 FROM %s %s WHERE %s.%s = %s.%s AND `,
			quoteIdentifier(parentCfg.TableName), alias,
			alias, quoteIdentifier(parentCfg.IDColumn),
			childRef, quoteIdentifier(cfg.ParentFKCol))

		var ownerPredicate string
		if includeGlobal {
			ownerPredicate = fmt.Sprintf("(%s.%s IS NULL OR %s.%s::text = current_setting('app.user_id', true))",
				alias, quoteIdentifier(parentCfg.OwnerColumn),
				alias, quoteIdentifier(parentCfg.OwnerColumn))
		} else {
			ownerPredicate = fmt.Sprintf("%s.%s::text = current_setting('app.user_id', true)",
				alias, quoteIdentifier(parentCfg.OwnerColumn))
		}
		return base + ownerPredicate + `)`
	}

	inner := buildParentOwnerCheckAt(registry, parentCfg, alias, depth+1, includeGlobal)
	if inner == "" {
		return ""
	}
	return fmt.Sprintf(`EXISTS (SELECT 1 FROM %s %s WHERE %s.%s = %s.%s AND %s)`,
		quoteIdentifier(parentCfg.TableName), alias,
		alias, quoteIdentifier(parentCfg.IDColumn),
		childRef, quoteIdentifier(cfg.ParentFKCol),
		inner)
}
