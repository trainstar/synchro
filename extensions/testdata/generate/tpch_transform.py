"""Transform TPC-H dbgen output into our modernized schema.

Reads pipe-delimited .tbl files, maps to UUID PKs, adds timestamptz,
jsonb, boolean, array columns. Outputs SQL INSERT statements.
"""

import csv
import hashlib
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


def deterministic_uuid(namespace: str, key: str) -> str:
    """Generate a reproducible UUID from a namespace + key."""
    h = hashlib.sha256(f"{namespace}:{key}".encode()).digest()
    return str(uuid.UUID(bytes=h[:16], version=4))


def sql_str(val: str | None) -> str:
    """Escape a string for SQL. Returns NULL for None."""
    if val is None:
        return "NULL"
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def sql_array(vals: list[str]) -> str:
    """Format a Python list as a PG text array literal."""
    if not vals:
        return "'{}'::text[]"
    items = ", ".join(sql_str(v) for v in vals)
    return f"ARRAY[{items}]"


def sql_int_array(vals: list[int]) -> str:
    if not vals:
        return "'{}'::integer[]"
    items = ", ".join(str(v) for v in vals)
    return f"ARRAY[{items}]"


def sql_jsonb(obj: dict | list) -> str:
    """Format a Python dict/list as a PG jsonb literal."""
    return f"'{json.dumps(obj, ensure_ascii=False).replace(chr(39), chr(39)+chr(39))}'::jsonb"


def random_ts(rng: random.Random, days_back: int = 90) -> str:
    """Random timestamptz within the last N days."""
    base = datetime(2025, 6, 15, tzinfo=timezone.utc)
    offset = timedelta(seconds=rng.randint(0, days_back * 86400))
    dt = base - offset
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def assign_user_ids(n_records: int, n_users: int, rng: random.Random) -> list[str]:
    """Assign user IDs with Zipf-like distribution.

    user-1 gets ~40%, user-2 ~25%, user-3 ~15%, rest split among user-4..N.
    """
    weights = [1.0 / (i + 1) for i in range(n_users)]
    total = sum(weights)
    weights = [w / total for w in weights]

    user_ids = [f"user-{i+1}" for i in range(n_users)]
    return rng.choices(user_ids, weights=weights, k=n_records)


def read_tbl(path: Path) -> list[list[str]]:
    """Read a pipe-delimited .tbl file (dbgen format).

    dbgen lines end with a trailing pipe, so we strip the last empty field.
    """
    rows = []
    with open(path, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            # Strip trailing empty field from dbgen's trailing pipe.
            if row and row[-1] == "":
                row = row[:-1]
            if row:
                rows.append(row)
    return rows


class TpchTransformer:
    def __init__(self, tbl_dir: Path, n_users: int = 3, seed: int = 42):
        self.tbl_dir = tbl_dir
        self.n_users = n_users
        self.rng = random.Random(seed)

        # Caches for FK resolution.
        self.customer_uuids: dict[str, str] = {}  # custkey -> uuid
        self.customer_user_ids: dict[str, str] = {}  # custkey -> user_id
        self.order_uuids: dict[str, str] = {}  # orderkey -> uuid
        self.part_uuids: dict[str, str] = {}  # partkey -> uuid
        self.supplier_uuids: dict[str, str] = {}  # suppkey -> uuid
        self.nation_uuids: dict[str, str] = {}  # nationkey -> uuid
        self.region_uuids: dict[str, str] = {}  # regionkey -> uuid

    def transform_regions(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "region.tbl")
        stmts = []
        for row in rows:
            regionkey, name, comment = row[0], row[1], row[2]
            uid = deterministic_uuid("region", regionkey)
            self.region_uuids[regionkey] = uid
            stmts.append(
                f"INSERT INTO regions (id, name, description) VALUES "
                f"({sql_str(uid)}, {sql_str(name.strip())}, {sql_str(comment.strip())});"
            )
        return stmts

    def transform_nations(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "nation.tbl")
        stmts = []
        for row in rows:
            nationkey, name, regionkey, comment = row[0], row[1], row[2], row[3]
            uid = deterministic_uuid("nation", nationkey)
            self.nation_uuids[nationkey] = uid
            region_uuid = self.region_uuids[regionkey]
            iso = name.strip()[:2].upper()
            metadata = {"comment": comment.strip()} if comment.strip() else {}
            stmts.append(
                f"INSERT INTO nations (id, region_id, name, iso_code, metadata) VALUES "
                f"({sql_str(uid)}, {sql_str(region_uuid)}, {sql_str(name.strip())}, "
                f"{sql_str(iso)}, {sql_jsonb(metadata)});"
            )
        return stmts

    def transform_suppliers(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "supplier.tbl")
        stmts = []
        for row in rows:
            suppkey = row[0]
            name, address, nationkey, phone = row[1], row[2], row[3], row[4]
            acctbal, comment = row[5], row[6]

            uid = deterministic_uuid("supplier", suppkey)
            self.supplier_uuids[suppkey] = uid
            nation_uuid = self.nation_uuids[nationkey]
            rating = round(self.rng.uniform(1.0, 5.0), 2)
            is_active = self.rng.random() > 0.1
            tags = []
            if float(acctbal) > 5000:
                tags.append("premium")
            if "express" in comment.lower() or "quick" in comment.lower():
                tags.append("fast-ship")

            stmts.append(
                f"INSERT INTO suppliers (id, nation_id, name, address, phone, "
                f"rating, is_active, tags) VALUES "
                f"({sql_str(uid)}, {sql_str(nation_uuid)}, {sql_str(name.strip())}, "
                f"{sql_str(address.strip())}, {sql_str(phone.strip())}, "
                f"{rating}, {str(is_active).lower()}, {sql_array(tags)});"
            )
        return stmts

    def transform_parts(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "part.tbl")
        stmts = []
        for row in rows:
            partkey = row[0]
            name, mfgr, brand, ptype = row[1], row[2], row[3], row[4]
            size, container, retailprice, comment = row[5], row[6], row[7], row[8]

            uid = deterministic_uuid("part", partkey)
            self.part_uuids[partkey] = uid

            specs = {"container": container.strip(), "size": int(size)}
            tags = [w.strip().lower() for w in ptype.strip().split() if len(w.strip()) > 2][:3]

            stmts.append(
                f"INSERT INTO parts (id, name, manufacturer, brand, part_type, "
                f"size_cm, weight_kg, retail_price, description, specifications, tags) VALUES "
                f"({sql_str(uid)}, {sql_str(name.strip())}, {sql_str(mfgr.strip())}, "
                f"{sql_str(brand.strip())}, {sql_str(ptype.strip())}, "
                f"{size}, {round(self.rng.uniform(0.01, 50.0), 3)}, {retailprice}, "
                f"{sql_str(comment.strip())}, {sql_jsonb(specs)}, {sql_array(tags)});"
            )
        return stmts

    def transform_part_suppliers(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "partsupp.tbl")
        stmts = []
        for row in rows:
            partkey, suppkey = row[0], row[1]
            availqty, supplycost, comment = row[2], row[3], row[4]

            uid = deterministic_uuid("partsupp", f"{partkey}:{suppkey}")
            part_uuid = self.part_uuids.get(partkey)
            supp_uuid = self.supplier_uuids.get(suppkey)
            if not part_uuid or not supp_uuid:
                continue

            lead = self.rng.randint(1, 30)
            notes = comment.strip() if self.rng.random() > 0.7 else None

            stmts.append(
                f"INSERT INTO part_suppliers (id, part_id, supplier_id, "
                f"available_quantity, supply_cost, lead_time_days, notes) VALUES "
                f"({sql_str(uid)}, {sql_str(part_uuid)}, {sql_str(supp_uuid)}, "
                f"{availqty}, {supplycost}, {lead}, {sql_str(notes)});"
            )
        return stmts

    def transform_customers(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "customer.tbl")
        user_ids = assign_user_ids(len(rows), self.n_users, self.rng)
        stmts = []

        for i, row in enumerate(rows):
            custkey = row[0]
            name, address, nationkey, phone = row[1], row[2], row[3], row[4]
            acctbal, mktsegment, comment = row[5], row[6], row[7]

            uid = deterministic_uuid("customer", custkey)
            self.customer_uuids[custkey] = uid
            user_id = user_ids[i]
            self.customer_user_ids[custkey] = user_id
            nation_uuid = self.nation_uuids.get(nationkey)

            addr_json = {"street": address.strip(), "raw": address.strip()}
            prefs = {}
            if mktsegment.strip() == "BUILDING":
                prefs["industry"] = "construction"
            elif mktsegment.strip() == "AUTOMOBILE":
                prefs["industry"] = "automotive"

            is_active = self.rng.random() > 0.05
            deleted_at = "NULL"
            if not is_active and self.rng.random() > 0.5:
                deleted_at = sql_str(random_ts(self.rng, 30))

            internal = comment.strip() if self.rng.random() > 0.8 else ""

            nation_val = sql_str(nation_uuid) if nation_uuid else "NULL"
            stmts.append(
                f"INSERT INTO customers (id, user_id, name, email, phone, address, "
                f"nation_id, balance, market_segment, is_active, preferences, "
                f"internal_notes, deleted_at) VALUES "
                f"({sql_str(uid)}, {sql_str(user_id)}, {sql_str(name.strip())}, "
                f"{sql_str(name.strip().lower().replace(' ', '.').replace('#', '') + '@example.com')}, "
                f"{sql_str(phone.strip())}, {sql_jsonb(addr_json)}, "
                f"{nation_val}, {acctbal}, {sql_str(mktsegment.strip())}, "
                f"{str(is_active).lower()}, {sql_jsonb(prefs)}, "
                f"{sql_str(internal)}, {deleted_at});"
            )
        return stmts

    def transform_orders(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "orders.tbl")
        stmts = []

        status_map = {
            "O": "pending",
            "F": "completed",
            "P": "processing",
        }
        priority_map = {
            "1-URGENT": "high",
            "2-HIGH": "high",
            "3-MEDIUM": "normal",
            "4-NOT SPECIFIED": "normal",
            "5-LOW": "low",
        }

        for row in rows:
            orderkey = row[0]
            custkey, orderstatus, totalprice = row[1], row[2], row[3]
            orderdate, orderpriority = row[4], row[5]
            clerk, shippriority, comment = row[6], row[7], row[8]

            uid = deterministic_uuid("order", orderkey)
            self.order_uuids[orderkey] = uid
            cust_uuid = self.customer_uuids.get(custkey)
            if not cust_uuid:
                continue

            status = status_map.get(orderstatus.strip(), "pending")
            priority = priority_map.get(orderpriority.strip(), "normal")
            ship_date = "NULL"
            if status == "completed":
                ship_date = sql_str(orderdate.strip())

            ship_addr = None
            if status in ("completed", "shipped"):
                ship_addr = {"clerk": clerk.strip()}

            tags = []
            if priority == "high":
                tags.append("rush")
            if float(totalprice) > 200000:
                tags.append("large-order")

            deleted_at = "NULL"
            if status == "cancelled" or (status == "pending" and self.rng.random() < 0.03):
                if self.rng.random() < 0.5:
                    deleted_at = sql_str(random_ts(self.rng, 60))

            ship_addr_sql = sql_jsonb(ship_addr) if ship_addr else "NULL"
            cmt = comment.strip() if comment.strip() else None

            stmts.append(
                f"INSERT INTO orders (id, customer_id, status, total_price, currency, "
                f"priority, ship_date, ship_address, order_comment, tags, deleted_at) VALUES "
                f"({sql_str(uid)}, {sql_str(cust_uuid)}, {sql_str(status)}, "
                f"{totalprice}, 'USD', {sql_str(priority)}, {ship_date}, "
                f"{ship_addr_sql}, {sql_str(cmt)}, {sql_array(tags)}, {deleted_at});"
            )
        return stmts

    def transform_line_items(self) -> list[str]:
        rows = read_tbl(self.tbl_dir / "lineitem.tbl")
        stmts = []

        status_map = {"O": "open", "F": "delivered"}

        for row in rows:
            orderkey, partkey, suppkey = row[0], row[1], row[2]
            linenumber, quantity, extendedprice = row[3], row[4], row[5]
            discount, tax, returnflag = row[6], row[7], row[8]
            linestatus, shipdate, commitdate = row[9], row[10], row[11]
            receiptdate, shipinstruct, shipmode = row[12], row[13], row[14]
            comment = row[15]

            uid = deterministic_uuid("lineitem", f"{orderkey}:{linenumber}")
            order_uuid = self.order_uuids.get(orderkey)
            part_uuid = self.part_uuids.get(partkey)
            if not order_uuid:
                continue

            unit_price = round(float(extendedprice) / max(int(quantity), 1), 2)
            ls = status_map.get(linestatus.strip(), "open")
            if returnflag.strip() == "R":
                ls = "returned"

            ship_dt = sql_str(shipdate.strip()) if shipdate.strip() else "NULL"
            receipt_dt = sql_str(receiptdate.strip()) if receiptdate.strip() else "NULL"
            cmt = comment.strip() if comment.strip() and self.rng.random() > 0.8 else None

            deleted_at = "NULL"
            if ls == "returned" and self.rng.random() < 0.1:
                deleted_at = sql_str(random_ts(self.rng, 30))

            part_val = sql_str(part_uuid) if part_uuid else "NULL"
            stmts.append(
                f"INSERT INTO line_items (id, order_id, part_id, quantity, "
                f"unit_price, discount, tax, line_status, ship_date, receipt_date, "
                f"ship_instructions, line_comment, deleted_at) VALUES "
                f"({sql_str(uid)}, {sql_str(order_uuid)}, {part_val}, "
                f"{quantity}, {unit_price}, {discount}, {tax}, {sql_str(ls)}, "
                f"{ship_dt}, {receipt_dt}, {sql_str(shipinstruct.strip())}, "
                f"{sql_str(cmt)}, {deleted_at});"
            )
        return stmts
