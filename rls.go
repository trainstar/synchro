package synchro

import (
	"fmt"
)

// GenerateRLSPolicies generates SQL statements to enable RLS and create
// policies for all registered tables based on registry configuration.
func GenerateRLSPolicies(registry *Registry) []string {
	var stmts []string

	for _, cfg := range registry.All() {
		if cfg.Direction == ServerOnly {
			// ServerOnly tables get a read-all policy, no write
			stmts = append(stmts,
				fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", quoteIdentifier(cfg.TableName)),
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (true)`,
					quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName)),
			)
			continue
		}

		if cfg.OwnerColumn != "" {
			stmts = append(stmts,
				fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", quoteIdentifier(cfg.TableName)),
			)

			// Read policy
			if cfg.Direction == SystemAndUser {
				// SystemAndUser: read system (NULL owner) + own records
				stmts = append(stmts,
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (%s IS NULL OR %s = current_setting('app.user_id')::uuid)`,
						quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName),
						quoteIdentifier(cfg.OwnerColumn), quoteIdentifier(cfg.OwnerColumn)),
				)
			} else {
				// Bidirectional: only own records
				stmts = append(stmts,
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (%s = current_setting('app.user_id')::uuid)`,
						quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName),
						quoteIdentifier(cfg.OwnerColumn)),
				)
			}

			// Write policy (INSERT/UPDATE/DELETE): must be own records
			stmts = append(stmts,
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR INSERT WITH CHECK (%s = current_setting('app.user_id')::uuid)`,
					quoteIdentifier("sync_write_ins_"+cfg.TableName), quoteIdentifier(cfg.TableName),
					quoteIdentifier(cfg.OwnerColumn)),
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR UPDATE USING (%s = current_setting('app.user_id')::uuid)`,
					quoteIdentifier("sync_write_upd_"+cfg.TableName), quoteIdentifier(cfg.TableName),
					quoteIdentifier(cfg.OwnerColumn)),
				fmt.Sprintf(`CREATE POLICY %s ON %s FOR DELETE USING (%s = current_setting('app.user_id')::uuid)`,
					quoteIdentifier("sync_write_del_"+cfg.TableName), quoteIdentifier(cfg.TableName),
					quoteIdentifier(cfg.OwnerColumn)),
			)
			continue
		}

		if cfg.ParentTable != "" {
			// Child table: policy chains through parent
			parentCfg := registry.Get(cfg.ParentTable)
			if parentCfg == nil {
				continue
			}

			stmts = append(stmts,
				fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", quoteIdentifier(cfg.TableName)),
			)

			// Build the ownership check as a subquery through the parent chain
			ownerCheck := buildParentOwnerCheck(registry, cfg)
			if ownerCheck != "" {
				stmts = append(stmts,
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR SELECT USING (%s)`,
						quoteIdentifier("sync_read_"+cfg.TableName), quoteIdentifier(cfg.TableName), ownerCheck),
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR INSERT WITH CHECK (%s)`,
						quoteIdentifier("sync_write_ins_"+cfg.TableName), quoteIdentifier(cfg.TableName), ownerCheck),
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR UPDATE USING (%s)`,
						quoteIdentifier("sync_write_upd_"+cfg.TableName), quoteIdentifier(cfg.TableName), ownerCheck),
					fmt.Sprintf(`CREATE POLICY %s ON %s FOR DELETE USING (%s)`,
						quoteIdentifier("sync_write_del_"+cfg.TableName), quoteIdentifier(cfg.TableName), ownerCheck),
				)
			}
		}
	}

	return stmts
}

// buildParentOwnerCheck builds a nested EXISTS subquery for RLS that
// chains through parent tables to verify ownership.
// childRef is the SQL expression for the child table (raw table name at depth 0,
// alias at deeper levels). depth tracks nesting for unique aliases.
func buildParentOwnerCheck(registry *Registry, cfg *TableConfig) string {
	// The policy is ON cfg.TableName, so at the top level the child is
	// referenced by its raw table name (RLS policies have implicit access).
	return buildParentOwnerCheckAt(registry, cfg, quoteIdentifier(cfg.TableName), 1)
}

func buildParentOwnerCheckAt(registry *Registry, cfg *TableConfig, childRef string, depth int) string {
	if cfg.ParentTable == "" {
		return ""
	}

	parentCfg := registry.Get(cfg.ParentTable)
	if parentCfg == nil {
		return ""
	}

	alias := fmt.Sprintf("p%d", depth)

	if parentCfg.OwnerColumn != "" {
		check := fmt.Sprintf(`EXISTS (SELECT 1 FROM %s %s WHERE %s.%s = %s.%s`,
			quoteIdentifier(parentCfg.TableName), alias,
			alias, quoteIdentifier(parentCfg.IDColumn),
			childRef, quoteIdentifier(cfg.ParentFKCol))

		if parentCfg.Direction == SystemAndUser {
			check += fmt.Sprintf(` AND (%s.%s IS NULL OR %s.%s = current_setting('app.user_id')::uuid))`,
				alias, quoteIdentifier(parentCfg.OwnerColumn),
				alias, quoteIdentifier(parentCfg.OwnerColumn))
		} else {
			check += fmt.Sprintf(` AND %s.%s = current_setting('app.user_id')::uuid)`,
				alias, quoteIdentifier(parentCfg.OwnerColumn))
		}
		return check
	}

	// Parent is also a child — recurse with the current alias as the new childRef
	innerCheck := buildParentOwnerCheckAt(registry, parentCfg, alias, depth+1)
	if innerCheck == "" {
		return ""
	}

	return fmt.Sprintf(`EXISTS (SELECT 1 FROM %s %s WHERE %s.%s = %s.%s AND %s)`,
		quoteIdentifier(parentCfg.TableName), alias,
		alias, quoteIdentifier(parentCfg.IDColumn),
		childRef, quoteIdentifier(cfg.ParentFKCol),
		innerCheck)
}
