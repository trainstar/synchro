#!/usr/bin/env python3
"""Generate test seed SQL from TPC-H dbgen output.

Usage:
    # Build dbgen and generate seed (default SF 0.1, 3 users)
    python3 generate.py

    # Custom scale and users
    python3 generate.py --scale 0.1 --users 5

    # Use existing dbgen output
    python3 generate.py --tbl-dir /path/to/tbl/files
"""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Add this directory to path for local imports.
sys.path.insert(0, str(Path(__file__).parent))

from tpch_transform import TpchTransformer
from collaboration import (
    generate_categories,
    generate_documents,
    generate_document_members,
    generate_document_comments,
)
from type_zoo import generate_type_zoo


DBGEN_REPO = "https://github.com/electrum/tpch-dbgen.git"


def build_dbgen(build_dir: Path) -> Path:
    """Clone and build tpch-dbgen. Returns path to dbgen binary."""
    dbgen_path = build_dir / "dbgen"
    if dbgen_path.exists():
        return dbgen_path

    print(f"Cloning tpch-dbgen into {build_dir}...", file=sys.stderr)
    subprocess.run(
        ["git", "clone", "--depth", "1", DBGEN_REPO, str(build_dir)],
        check=True,
        capture_output=True,
    )

    print("Building dbgen...", file=sys.stderr)
    subprocess.run(
        ["make", "-j", str(os.cpu_count() or 4)],
        cwd=build_dir,
        check=True,
        capture_output=True,
    )

    if not dbgen_path.exists():
        print("ERROR: dbgen binary not found after build", file=sys.stderr)
        sys.exit(1)

    return dbgen_path


def run_dbgen(dbgen_path: Path, scale: float, output_dir: Path) -> None:
    """Run dbgen to generate .tbl files."""
    print(f"Running dbgen (scale={scale})...", file=sys.stderr)
    subprocess.run(
        [str(dbgen_path), "-s", str(scale), "-f"],
        cwd=output_dir,
        check=True,
        capture_output=True,
    )

    # Verify output files exist.
    expected = ["region.tbl", "nation.tbl", "customer.tbl", "orders.tbl",
                "lineitem.tbl", "part.tbl", "supplier.tbl", "partsupp.tbl"]
    for f in expected:
        if not (output_dir / f).exists():
            print(f"ERROR: dbgen did not produce {f}", file=sys.stderr)
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Generate test seed SQL from TPC-H data")
    parser.add_argument("--scale", type=float, default=0.1,
                        help="TPC-H scale factor (default: 0.1)")
    parser.add_argument("--users", type=int, default=3,
                        help="Number of test users (default: 3)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for deterministic output (default: 42)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output SQL file (default: ../seed.sql)")
    parser.add_argument("--tbl-dir", type=str, default=None,
                        help="Directory with pre-generated .tbl files (skips dbgen)")
    parser.add_argument("--dbgen-dir", type=str, default=None,
                        help="Directory to clone/build dbgen (default: /tmp/tpch-dbgen)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    output_path = Path(args.output) if args.output else script_dir.parent / "seed.sql"

    # Step 1: Get TPC-H .tbl files.
    if args.tbl_dir:
        tbl_dir = Path(args.tbl_dir)
    else:
        dbgen_dir = Path(args.dbgen_dir) if args.dbgen_dir else Path("/tmp/tpch-dbgen")
        dbgen_path = build_dbgen(dbgen_dir)

        # Generate into a temp directory (dbgen outputs to cwd).
        tbl_dir = dbgen_dir
        run_dbgen(dbgen_path, args.scale, tbl_dir)

    # Step 2: Transform TPC-H data.
    print("Transforming TPC-H data...", file=sys.stderr)
    transformer = TpchTransformer(tbl_dir, n_users=args.users, seed=args.seed)

    all_stmts: list[str] = []
    all_stmts.append("-- Generated test seed from TPC-H (scale={}, users={}, seed={})".format(
        args.scale, args.users, args.seed))
    all_stmts.append("-- Do not edit manually. Regenerate with: python3 generate.py")
    all_stmts.append("")
    all_stmts.append("BEGIN;")
    all_stmts.append("")

    # Reference tables (FK dependency order).
    all_stmts.append("-- Regions")
    all_stmts.extend(transformer.transform_regions())
    all_stmts.append("")

    all_stmts.append("-- Nations")
    all_stmts.extend(transformer.transform_nations())
    all_stmts.append("")

    all_stmts.append("-- Suppliers")
    all_stmts.extend(transformer.transform_suppliers())
    all_stmts.append("")

    all_stmts.append("-- Parts")
    all_stmts.extend(transformer.transform_parts())
    all_stmts.append("")

    all_stmts.append("-- Part-Supplier relationships")
    all_stmts.extend(transformer.transform_part_suppliers())
    all_stmts.append("")

    # Categories (self-referential, no TPC-H dependency).
    print("Generating categories...", file=sys.stderr)
    import random
    rng = random.Random(args.seed)
    all_stmts.append("-- Categories")
    all_stmts.extend(generate_categories(rng))
    all_stmts.append("")

    # User-owned tables.
    all_stmts.append("-- Customers")
    all_stmts.extend(transformer.transform_customers())
    all_stmts.append("")

    all_stmts.append("-- Orders")
    all_stmts.extend(transformer.transform_orders())
    all_stmts.append("")

    all_stmts.append("-- Line Items")
    all_stmts.extend(transformer.transform_line_items())
    all_stmts.append("")

    # Collaboration tables (derived from TPC-H customers).
    print("Generating collaboration data...", file=sys.stderr)
    doc_stmts, doc_owners, doc_members = generate_documents(
        transformer.customer_user_ids, args.users, rng
    )
    all_stmts.append("-- Documents")
    all_stmts.extend(doc_stmts)
    all_stmts.append("")

    all_stmts.append("-- Document Members")
    all_stmts.extend(generate_document_members(doc_members, rng))
    all_stmts.append("")

    all_stmts.append("-- Document Comments")
    all_stmts.extend(generate_document_comments(doc_owners, doc_members, rng))
    all_stmts.append("")

    # Type zoo.
    print("Generating type_zoo...", file=sys.stderr)
    user_ids = [f"user-{i+1}" for i in range(args.users)]
    all_stmts.append("-- Type Zoo")
    all_stmts.extend(generate_type_zoo(user_ids))
    all_stmts.append("")

    all_stmts.append("COMMIT;")

    # Write output.
    output_path.write_text("\n".join(all_stmts) + "\n", encoding="utf-8")

    # Count rows.
    insert_count = sum(1 for s in all_stmts if s.startswith("INSERT"))
    print(f"Generated {insert_count} rows -> {output_path}", file=sys.stderr)
    print(f"  Scale: {args.scale}, Users: {args.users}, Seed: {args.seed}", file=sys.stderr)


if __name__ == "__main__":
    main()
