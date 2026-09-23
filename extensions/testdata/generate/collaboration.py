"""Generate collaboration tables from TPC-H customer data.

Documents, members, comments, categories. Derived from TPC-H customers
so all user_ids link back to real data.
"""

import random

from tpch_transform import deterministic_uuid, sql_str, sql_jsonb, random_ts


def generate_categories(rng: random.Random) -> list[str]:
    """Generate a 3-level category hierarchy (~25 categories)."""
    stmts = []
    cat_id = 0

    top_level = [
        ("Electronics", {"icon": "cpu"}),
        ("Industrial", {"icon": "wrench"}),
        ("Office Supplies", {"icon": "pencil"}),
    ]

    children = {
        "Electronics": [
            "Semiconductors", "Displays", "Cables", "Power Supplies"
        ],
        "Industrial": [
            "Bearings", "Fasteners", "Belts", "Seals"
        ],
        "Office Supplies": [
            "Paper", "Writing", "Filing"
        ],
    }

    grandchildren = {
        "Semiconductors": ["Memory Chips", "Processors", "FPGAs"],
        "Displays": ["LCD Panels", "OLED Panels"],
        "Bearings": ["Ball Bearings", "Roller Bearings", "Thrust Bearings"],
        "Fasteners": ["Bolts", "Nuts", "Screws", "Washers"],
    }

    parent_uuids: dict[str, str] = {}

    for sort_order, (name, metadata) in enumerate(top_level, 1):
        cat_id += 1
        uid = deterministic_uuid("category", str(cat_id))
        parent_uuids[name] = uid
        stmts.append(
            f"INSERT INTO categories (id, parent_id, name, sort_order, metadata) VALUES "
            f"({sql_str(uid)}, NULL, {sql_str(name)}, {sort_order}, {sql_jsonb(metadata)});"
        )

    for parent_name, child_names in children.items():
        parent_uuid = parent_uuids[parent_name]
        for sort_order, child_name in enumerate(child_names, 1):
            cat_id += 1
            uid = deterministic_uuid("category", str(cat_id))
            parent_uuids[child_name] = uid
            stmts.append(
                f"INSERT INTO categories (id, parent_id, name, sort_order, metadata) VALUES "
                f"({sql_str(uid)}, {sql_str(parent_uuid)}, {sql_str(child_name)}, "
                f"{sort_order}, {sql_jsonb({})});"
            )

    for parent_name, gc_names in grandchildren.items():
        parent_uuid = parent_uuids.get(parent_name)
        if not parent_uuid:
            continue
        for sort_order, gc_name in enumerate(gc_names, 1):
            cat_id += 1
            uid = deterministic_uuid("category", str(cat_id))
            stmts.append(
                f"INSERT INTO categories (id, parent_id, name, sort_order, metadata) VALUES "
                f"({sql_str(uid)}, {sql_str(parent_uuid)}, {sql_str(gc_name)}, "
                f"{sort_order}, {sql_jsonb({})});"
            )

    return stmts


def generate_documents(
    customer_user_ids: dict[str, str],
    n_users: int,
    rng: random.Random,
) -> tuple[list[str], dict[str, str], dict[str, list[str]]]:
    """Generate documents owned by TPC-H customers.

    Returns (sql_statements, doc_id_to_owner, doc_id_to_member_user_ids).
    """
    user_ids = sorted(set(customer_user_ids.values()))
    if not user_ids:
        return [], {}, {}

    # 1 doc per 10 customers, min 10 docs.
    n_docs = max(10, len(customer_user_ids) // 10)

    stmts = []
    doc_owners: dict[str, str] = {}
    doc_members: dict[str, list[str]] = {}

    doc_types = ["note", "report", "spreadsheet", "presentation"]
    titles = [
        "Q{q} Forecast", "Supplier Review", "Inventory Plan",
        "Budget Proposal", "Team Standup Notes", "Product Roadmap",
        "Meeting Minutes", "Design Spec", "Test Results",
        "Incident Report", "Release Notes", "Onboarding Guide",
        "Performance Review", "Architecture Decision", "Risk Assessment",
    ]

    for i in range(n_docs):
        uid = deterministic_uuid("document", str(i))
        owner = rng.choice(user_ids)
        doc_owners[uid] = owner

        # Zipf-like member count: most 1-2 members, few 5-10.
        n_members = min(len(user_ids), max(1, int(rng.paretovariate(1.5))))
        members = [owner]
        other_users = [u for u in user_ids if u != owner]
        if other_users and n_members > 1:
            extra = rng.sample(other_users, min(n_members - 1, len(other_users)))
            members.extend(extra)
        doc_members[uid] = members

        title = rng.choice(titles).format(q=rng.randint(1, 4))
        dtype = rng.choice(doc_types)
        is_public = rng.random() < 0.1
        version = rng.randint(1, 20)
        metadata = {}
        if rng.random() > 0.5:
            metadata["department"] = rng.choice(["engineering", "sales", "ops", "finance"])

        deleted_at = "NULL"
        if rng.random() < 0.05:
            deleted_at = sql_str(random_ts(rng, 30))

        stmts.append(
            f"INSERT INTO documents (id, owner_id, title, content, doc_type, "
            f"is_public, version_number, metadata, deleted_at) VALUES "
            f"({sql_str(uid)}, {sql_str(owner)}, {sql_str(title)}, "
            f"{sql_str(f'Content for {title}...')}, {sql_str(dtype)}, "
            f"{str(is_public).lower()}, {version}, {sql_jsonb(metadata)}, {deleted_at});"
        )

    return stmts, doc_owners, doc_members


def generate_document_members(
    doc_members: dict[str, list[str]],
    rng: random.Random,
) -> list[str]:
    """Generate document_members rows from the membership map."""
    stmts = []
    member_id = 0
    roles = ["owner", "editor", "viewer", "commenter"]

    for doc_id, members in doc_members.items():
        for i, user_id in enumerate(members):
            member_id += 1
            uid = deterministic_uuid("docmember", str(member_id))
            role = "owner" if i == 0 else rng.choice(roles[1:])

            deleted_at = "NULL"
            if role != "owner" and rng.random() < 0.03:
                deleted_at = sql_str(random_ts(rng, 15))

            stmts.append(
                f"INSERT INTO document_members (id, document_id, user_id, role, deleted_at) VALUES "
                f"({sql_str(uid)}, {sql_str(doc_id)}, {sql_str(user_id)}, "
                f"{sql_str(role)}, {deleted_at});"
            )

    return stmts


def generate_document_comments(
    doc_owners: dict[str, str],
    doc_members: dict[str, list[str]],
    rng: random.Random,
) -> list[str]:
    """Generate threaded comments on documents.

    Authors are drawn from document members (realistic).
    Thread depth follows geometric distribution.
    """
    stmts = []
    comment_id = 0
    comment_bodies = [
        "Looks good to me.",
        "Can we revisit the numbers in section 3?",
        "Updated with latest data.",
        "Agreed, let's proceed.",
        "I have concerns about the timeline.",
        "Fixed the formatting issue.",
        "Added the missing supplier data.",
        "Please review before EOD.",
        "This needs legal review.",
        "Great work on this.",
        "One minor correction needed.",
        "Approved.",
    ]

    # Track comment UUIDs per document for threading.
    doc_comment_uuids: dict[str, list[str]] = {}

    for doc_id, members in doc_members.items():
        if not members:
            continue

        # 2-8 comments per document, geometric distribution.
        n_comments = min(8, max(2, int(rng.expovariate(0.3))))
        doc_comment_uuids[doc_id] = []

        for _ in range(n_comments):
            comment_id += 1
            uid = deterministic_uuid("doccomment", str(comment_id))
            author = rng.choice(members)

            # Thread depth: 80% top-level, 15% reply, 5% nested reply.
            parent_id = "NULL"
            existing = doc_comment_uuids[doc_id]
            if existing:
                roll = rng.random()
                if roll > 0.8:
                    parent_id = sql_str(rng.choice(existing))

            body = rng.choice(comment_bodies)
            is_resolved = rng.random() < 0.2

            deleted_at = "NULL"
            if rng.random() < 0.02:
                deleted_at = sql_str(random_ts(rng, 10))

            doc_comment_uuids[doc_id].append(uid)

            stmts.append(
                f"INSERT INTO document_comments (id, document_id, author_id, "
                f"parent_comment_id, body, is_resolved, deleted_at) VALUES "
                f"({sql_str(uid)}, {sql_str(doc_id)}, {sql_str(author)}, "
                f"{parent_id}, {sql_str(body)}, {str(is_resolved).lower()}, {deleted_at});"
            )

    return stmts
