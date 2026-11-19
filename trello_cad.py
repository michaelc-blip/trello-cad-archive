#!/usr/bin/env python3
"""
Trello CAD archive: index every attachment in the Cullen Trello workspace,
report on it, then download the CAD / render / PDF files with a local SQLite
catalogue. Runs locally; sync the output folder to Google Drive with rclone.

  python3 trello_cad.py index              # metadata only, no downloads
  python3 trello_cad.py report             # counts + sizes -> report.md
  python3 trello_cad.py download [--board X] [--categories cad,render,pdf,archive]
  python3 trello_cad.py export             # catalogue.csv

Needs env vars TRELLO_API_KEY and TRELLO_TOKEN (read-only token is enough).
"""
import argparse, csv, hashlib, json, os, re, shutil, sqlite3, sys, threading, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse

import requests

API = "https://api.trello.com/1"
WORKSPACE = os.environ.get("TRELLO_WORKSPACE", "cullenjewellery2")
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("CAD_DB", os.path.join(HERE, "cad_archive.sqlite"))
OUT_DIR = os.environ.get("CAD_OUT", os.path.join(HERE, "archive"))

CATEGORIES = {
    "cad": {".3dm", ".3dmbak", ".stl", ".obj", ".stp", ".step", ".igs", ".iges",
            ".jcd", ".mgx", ".3ds", ".fbx", ".dxf", ".dwg", ".sldprt", ".x_t",
            ".ply", ".gdpx", ".3mf", ".skp"},
    "render": {".jpg", ".jpeg", ".png", ".gif", ".webp", ".tif", ".tiff", ".bmp",
               ".heic", ".psd"},
    "pdf": {".pdf"},
    "archive": {".zip", ".rar", ".7z"},
}
DEFAULT_DOWNLOAD = ["cad", "render", "pdf", "archive"]

# Trello: 100 requests / 10 s per token. Stay under it.
REQ_PER_SEC = float(os.environ.get("TRELLO_RPS", "8"))


# --------------------------------------------------------------------------- http
class Limiter:
    def __init__(self, rate):
        self.interval, self.lock, self.next = 1.0 / rate, threading.Lock(), 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next)
            self.next = t + self.interval
        if t > now:
            time.sleep(t - now)


LIMITER = Limiter(REQ_PER_SEC)
_local = threading.local()


def auth_header():
    key, token = os.environ.get("TRELLO_API_KEY"), os.environ.get("TRELLO_TOKEN")
    if not key or not token:
        sys.exit("Set TRELLO_API_KEY and TRELLO_TOKEN environment variables.")
    return {"Authorization": f'OAuth oauth_consumer_key="{key}", oauth_token="{token}"'}


def session():
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
        _local.s.headers.update(auth_header())
    return _local.s


def get(path_or_url, params=None, stream=False):
    url = path_or_url if path_or_url.startswith("http") else API + path_or_url
    for attempt in range(8):
        LIMITER.wait()
        try:
            r = session().get(url, params=params, stream=stream, timeout=300)
        except requests.RequestException as e:
            wait = min(60, 2 ** attempt)
            print(f"  network error ({e.__class__.__name__}), retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if r.status_code == 429 or r.status_code >= 500:
            wait = min(60, 2 ** (attempt + 1))
            print(f"  HTTP {r.status_code}, retry in {wait}s", file=sys.stderr)
            r.close()
            time.sleep(wait)
            continue
        return r
    raise RuntimeError(f"gave up on {url}")


def get_json(path, params=None):
    r = get(path, params)
    r.raise_for_status()
    return r.json()


# --------------------------------------------------------------------------- db
SCHEMA = """
CREATE TABLE IF NOT EXISTS boards(
  id TEXT PRIMARY KEY, name TEXT, closed INT, url TEXT, workspace_id TEXT,
  date_last_activity TEXT, supplier TEXT, indexed_at TEXT);
CREATE TABLE IF NOT EXISTS lists(
  id TEXT PRIMARY KEY, board_id TEXT, name TEXT, closed INT);
CREATE TABLE IF NOT EXISTS cards(
  id TEXT PRIMARY KEY, board_id TEXT, list_id TEXT, short_link TEXT, url TEXT,
  name TEXT, description TEXT, closed INT, deleted INT DEFAULT 0,
  date_created TEXT, date_last_activity TEXT, labels TEXT, custom_fields TEXT);
CREATE TABLE IF NOT EXISTS attachments(
  id TEXT PRIMARY KEY, card_id TEXT, board_id TEXT, name TEXT, file_name TEXT,
  url TEXT, mime TEXT, bytes INT, is_upload INT, date TEXT, member_id TEXT,
  ext TEXT, category TEXT, source TEXT, removed INT DEFAULT 0,
  status TEXT DEFAULT 'pending', sha256 TEXT, local_path TEXT, duplicate_of TEXT,
  error TEXT, downloaded_at TEXT);
CREATE INDEX IF NOT EXISTS ix_att_sha ON attachments(sha256);
CREATE INDEX IF NOT EXISTS ix_att_status ON attachments(status, category);
CREATE INDEX IF NOT EXISTS ix_card_board ON cards(board_id);
"""


def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def id_to_date(trello_id):
    return datetime.fromtimestamp(int(trello_id[:8], 16), timezone.utc).isoformat()


# versioned saves, e.g. "ring.jcd-20250704-2" -> ".jcd"
VERSION_SUFFIX = re.compile(r"-\d{8}(-\d+)?$")


def categorize(file_name, is_upload):
    ext = os.path.splitext(file_name or "")[1].lower()
    if not is_upload:
        return ext, "link"
    base = VERSION_SUFFIX.sub("", ext)
    for cat, exts in CATEGORIES.items():
        if ext in exts or base in exts:
            return ext, cat
    return ext, "other"


def cmd_recategorize(args):
    """Re-apply categorize() to every attachment already in the catalogue."""
    con = db()
    changed = 0
    for r in con.execute("SELECT id, file_name, is_upload, category FROM attachments").fetchall():
        ext, cat = categorize(r["file_name"], r["is_upload"])
        if cat != r["category"]:
            con.execute("UPDATE attachments SET ext=?, category=? WHERE id=?", (ext, cat, r["id"]))
            changed += 1
    con.commit()
    print(f"recategorised {changed} attachments")


def supplier_from_board(name):
    m = re.search(r"\+\s*(.+)$", name or "")
    return m.group(1).strip() if m else None


def upsert_attachment(con, a, card_id, board_id, source, removed=0):
    file_name = a.get("fileName") or a.get("name") or ""
    is_upload = 1 if a.get("isUpload", source == "action") else 0
    if source == "action":  # actions don't say isUpload; infer from URL host
        host = urlparse(a.get("url") or "").netloc
        is_upload = 1 if ("trello" in host or "amazonaws" in host) else 0
        file_name = file_name or unquote((a.get("url") or "").rsplit("/", 1)[-1])
    ext, cat = categorize(file_name, is_upload)
    con.execute("""
      INSERT INTO attachments(id, card_id, board_id, name, file_name, url, mime, bytes,
        is_upload, date, member_id, ext, category, source, removed)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(id) DO UPDATE SET card_id=excluded.card_id, board_id=excluded.board_id,
        name=excluded.name, url=excluded.url, ext=excluded.ext, category=excluded.category, bytes=COALESCE(excluded.bytes, bytes),
        removed=MIN(removed, excluded.removed)
      WHERE excluded.source='card' OR attachments.source='action'
    """, (a["id"], card_id, board_id, a.get("name"), file_name, a.get("url"),
          a.get("mimeType"), a.get("bytes"), is_upload, a.get("date"),
          a.get("idMember"), ext, cat, source, removed))


# --------------------------------------------------------------------------- index
def list_boards():
    fields = "name,closed,url,dateLastActivity,idOrganization"
    boards = {b["id"]: b for b in get_json(f"/organizations/{WORKSPACE}/boards",
                                           {"filter": "all", "fields": fields})}
    # boards the token owner belongs to outside the workspace (e.g. supplier-owned)
    for b in get_json("/members/me/boards", {"filter": "all", "fields": fields}):
        boards.setdefault(b["id"], b)
    return list(boards.values())


def index_board(con, b):
    bid = b["id"]
    print(f"\n== {b['name']}{' (closed)' if b.get('closed') else ''}")
    con.execute("""INSERT OR REPLACE INTO boards VALUES(?,?,?,?,?,?,?,?)""",
                (bid, b["name"], int(b.get("closed", False)), b.get("url"),
                 b.get("idOrganization"), b.get("dateLastActivity"),
                 supplier_from_board(b["name"]), datetime.now(timezone.utc).isoformat()))

    for l in get_json(f"/boards/{bid}/lists", {"filter": "all", "fields": "name,closed"}):
        con.execute("INSERT OR REPLACE INTO lists VALUES(?,?,?,?)",
                    (l["id"], bid, l["name"], int(l["closed"])))

    cf_defs = {f["id"]: f for f in get_json(f"/boards/{bid}/customFields")}

    def cf_value(item):
        f = cf_defs.get(item.get("idCustomField"), {})
        if item.get("idValue"):
            opt = next((o for o in f.get("options", []) if o["id"] == item["idValue"]), {})
            val = opt.get("value", {}).get("text")
        else:
            val = next(iter((item.get("value") or {}).values()), None)
        return f.get("name", item.get("idCustomField")), val

    # cards (open + archived), 1000 per page, attachments inline
    seen, before, n_cards, n_att = set(), None, 0, 0
    while True:
        params = {"limit": 1000, "attachments": "true", "attachment_fields": "all",
                  "customFieldItems": "true",
                  "fields": "name,desc,closed,idList,shortLink,shortUrl,dateLastActivity,labels"}
        if before:
            params["before"] = before
        batch = get_json(f"/boards/{bid}/cards/all", params)
        new = [c for c in batch if c["id"] not in seen]
        if not new:
            break
        for c in new:
            seen.add(c["id"])
            labels = ", ".join(l.get("name") or l.get("color") or "" for l in c.get("labels", []))
            cfs = dict(cf_value(i) for i in c.get("customFieldItems", []))
            con.execute("""INSERT OR REPLACE INTO cards(id, board_id, list_id, short_link, url,
                name, description, closed, deleted, date_created, date_last_activity, labels,
                custom_fields) VALUES(?,?,?,?,?,?,?,?,0,?,?,?,?)""",
                (c["id"], bid, c.get("idList"), c.get("shortLink"), c.get("shortUrl"),
                 c.get("name"), c.get("desc"), int(c.get("closed", False)),
                 id_to_date(c["id"]), c.get("dateLastActivity"), labels,
                 json.dumps(cfs, ensure_ascii=False) if cfs else None))
            for a in c.get("attachments", []):
                upsert_attachment(con, a, c["id"], bid, "card")
                n_att += 1
        n_cards += len(new)
        print(f"  cards: {n_cards}  attachments: {n_att}")
        con.commit()
        if len(batch) < 1000:
            break
        before = min(c["id"] for c in batch)

    # history: catches attachments removed from cards and cards since deleted
    before, n_hist = None, 0
    while True:
        params = {"filter": "addAttachmentToCard", "limit": 1000}
        if before:
            params["before"] = before
        acts = get_json(f"/boards/{bid}/actions", params)
        for act in acts:
            d = act.get("data", {})
            a, card = d.get("attachment"), d.get("card", {})
            if not a or not a.get("id"):
                continue
            exists = con.execute("SELECT 1 FROM attachments WHERE id=? AND source='card'",
                                 (a["id"],)).fetchone()
            if exists:
                continue
            if card.get("id") and not con.execute("SELECT 1 FROM cards WHERE id=?",
                                                  (card["id"],)).fetchone():
                con.execute("""INSERT INTO cards(id, board_id, short_link, name, closed, deleted,
                    date_created) VALUES(?,?,?,?,0,1,?)""",
                    (card["id"], bid, card.get("shortLink"), card.get("name"),
                     id_to_date(card["id"])))
            a = {**a, "date": act.get("date"), "idMember": act.get("idMemberCreator")}
            upsert_attachment(con, a, card.get("id"), bid, "action", removed=1)
            n_hist += 1
        con.commit()
        if len(acts) < 1000:
            break
        before = acts[-1]["id"]
    print(f"  removed/deleted attachments found in history: {n_hist}")


def cmd_index(args):
    con = db()
    boards = list_boards()
    print(f"{len(boards)} boards visible to this token")
    for b in boards:
        if args.board and args.board.lower() not in b["name"].lower():
            continue
        index_board(con, b)
    con.commit()
    cmd_report(args)


# --------------------------------------------------------------------------- report
def human(n):
    n = float(n or 0)
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"


def cmd_report(args):
    con = db()
    out = ["# Trello CAD archive - index report", "",
           f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", ""]
    tot = con.execute("""SELECT (SELECT COUNT(*) FROM boards), (SELECT COUNT(*) FROM cards),
        (SELECT SUM(closed) FROM cards), (SELECT SUM(deleted) FROM cards),
        (SELECT COUNT(*) FROM attachments)""").fetchone()
    out += [f"- Boards: {tot[0]}", f"- Cards: {tot[1]} (archived {tot[2] or 0}, deleted {tot[3] or 0})",
            f"- Attachments: {tot[4]}", ""]

    out += ["## By category", "", "| Category | Files | Size | Removed from card |", "|---|---:|---:|---:|"]
    for r in con.execute("""SELECT category, COUNT(*), SUM(bytes), SUM(removed) FROM attachments
                            GROUP BY category ORDER BY SUM(bytes) DESC"""):
        out.append(f"| {r[0]} | {r[1]} | {human(r[2])} | {r[3]} |")

    out += ["", "## By board (downloadable categories)", "",
            "| Board | Supplier | Cards | CAD | Render | PDF | Archive | Size |",
            "|---|---|---:|---:|---:|---:|---:|---:|"]
    for r in con.execute("""
        SELECT b.name, b.supplier, (SELECT COUNT(*) FROM cards c WHERE c.board_id=b.id),
          SUM(a.category='cad'), SUM(a.category='render'), SUM(a.category='pdf'),
          SUM(a.category='archive'),
          SUM(CASE WHEN a.category IN ('cad','render','pdf','archive') THEN a.bytes END)
        FROM boards b LEFT JOIN attachments a ON a.board_id=b.id
        GROUP BY b.id ORDER BY 8 DESC"""):
        out.append("| " + " | ".join(human(v) if i == 7 else str(v or 0 if i > 1 else v or "")
                                      for i, v in enumerate(r)) + " |")

    out += ["", "## By year (CAD files)", "", "| Year | CAD files | Size |", "|---|---:|---:|"]
    for r in con.execute("""SELECT substr(COALESCE(date,''),1,4) y, COUNT(*), SUM(bytes)
        FROM attachments WHERE category='cad' GROUP BY y ORDER BY y"""):
        out.append(f"| {r[0] or '?'} | {r[1]} | {human(r[2])} |")

    out += ["", "## File extensions", "", "| Ext | Category | Files | Size |", "|---|---|---:|---:|"]
    for r in con.execute("""SELECT ext, category, COUNT(*), SUM(bytes) FROM attachments
        GROUP BY ext, category ORDER BY COUNT(*) DESC LIMIT 60"""):
        out.append(f"| {r[0] or '(none)'} | {r[1]} | {r[2]} | {human(r[3])} |")

    domains = defaultdict(int)
    for (u,) in con.execute("SELECT url FROM attachments WHERE category='link'"):
        domains[urlparse(u or "").netloc or "?"] += 1
    out += ["", "## Linked (not uploaded) attachments - need separate retrieval", "",
            "| Domain | Links |", "|---|---:|"]
    for d, n in sorted(domains.items(), key=lambda x: -x[1])[:30]:
        out.append(f"| {d} | {n} |")

    st = con.execute("""SELECT status, COUNT(*), SUM(bytes) FROM attachments
        WHERE category IN ('cad','render','pdf','archive') GROUP BY status""").fetchall()
    out += ["", "## Download status", ""] + [f"- {r[0]}: {r[1]} ({human(r[2])})" for r in st]

    path = os.path.join(HERE, "report.md")
    with open(path, "w") as f:
        f.write("\n".join(out) + "\n")
    print("\n".join(out))
    print(f"\nwritten {path}")


# --------------------------------------------------------------------------- download
def slug(s, n=60):
    s = re.sub(r"[^\w\-. ]+", "", (s or "").strip()).strip().replace(" ", "-")
    return (s[:n] or "untitled").rstrip(".-")


def fetch(row, tmp_dir):
    tmp = os.path.join(tmp_dir, row["id"] + ".part")
    try:
        r = get(row["url"], stream=True)
        if r.status_code != 200:
            r.close()
            return row["id"], None, None, f"HTTP {r.status_code}"
        h, size = hashlib.sha256(), 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)
        if row["bytes"] and size != row["bytes"]:
            os.remove(tmp)
            return row["id"], None, None, f"size mismatch {size} != {row['bytes']}"
        return row["id"], tmp, h.hexdigest(), None
    except Exception as e:  # noqa: BLE001
        if os.path.exists(tmp):
            os.remove(tmp)
        return row["id"], None, None, f"{e.__class__.__name__}: {e}"


def cmd_download(args):
    con = db()
    cats = args.categories.split(",")
    q = f"""SELECT a.*, c.name card_name, c.short_link, b.name board_name, b.supplier
            FROM attachments a JOIN cards c ON c.id=a.card_id JOIN boards b ON b.id=a.board_id
            WHERE a.is_upload=1 AND a.category IN ({','.join('?' * len(cats))})
              AND a.status IN ({"'pending','failed'" if args.retry_failed else "'pending'"})"""
    params = list(cats)
    if args.board:
        q += " AND b.name LIKE ?"
        params.append(f"%{args.board}%")
    q += " ORDER BY a.date"
    rows = {r["id"]: r for r in con.execute(q, params)}
    total = sum(r["bytes"] or 0 for r in rows.values())
    print(f"{len(rows)} files to download, ~{human(total)}")
    if args.limit:
        rows = dict(list(rows.items())[:args.limit])

    tmp_dir = os.path.join(OUT_DIR, ".tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    done = failed = dup = 0
    with ThreadPoolExecutor(args.workers) as pool:
        futs = [pool.submit(fetch, r, tmp_dir) for r in rows.values()]
        for fut in as_completed(futs):
            aid, tmp, sha, err = fut.result()
            r = rows[aid]
            now = datetime.now(timezone.utc).isoformat()
            if err:
                failed += 1
                con.execute("UPDATE attachments SET status='failed', error=? WHERE id=?", (err, aid))
            else:
                prior = con.execute("""SELECT id, local_path FROM attachments
                    WHERE sha256=? AND status='done' AND duplicate_of IS NULL LIMIT 1""",
                    (sha,)).fetchone()
                if prior:
                    os.remove(tmp)
                    dup += 1
                    con.execute("""UPDATE attachments SET status='done', sha256=?, local_path=?,
                        duplicate_of=?, error=NULL, downloaded_at=? WHERE id=?""",
                        (sha, prior["local_path"], prior["id"], now, aid))
                else:
                    year = (r["date"] or id_to_date(r["card_id"]))[:4]
                    rel = os.path.join(slug(r["supplier"] or r["board_name"]), year,
                                       f"{r['short_link'] or r['card_id']}_{slug(r['card_name'])}",
                                       f"{(r['date'] or '')[:10]}_{slug(r['file_name'], 120)}")
                    dest = os.path.join(OUT_DIR, rel)
                    if os.path.exists(dest):
                        root, ext = os.path.splitext(dest)
                        dest, rel = f"{root}_{aid[-6:]}{ext}", f"{os.path.splitext(rel)[0]}_{aid[-6:]}{ext}"
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    shutil.move(tmp, dest)
                    done += 1
                    con.execute("""UPDATE attachments SET status='done', sha256=?, local_path=?,
                        error=NULL, downloaded_at=? WHERE id=?""", (sha, rel, now, aid))
            n = done + failed + dup
            if n % 25 == 0 or n == len(rows):
                con.commit()
                print(f"  {n}/{len(rows)}  saved {done}  duplicates {dup}  failed {failed}")
    con.commit()
    print(f"finished: saved {done}, duplicates {dup}, failed {failed}")
    cmd_export(args)


# --------------------------------------------------------------------------- export
def cmd_export(args):
    con = db()
    path = os.path.join(OUT_DIR, "catalogue.csv")
    os.makedirs(OUT_DIR, exist_ok=True)
    cols = ["attachment_id", "supplier", "board", "list", "card_name", "card_url", "card_archived",
            "card_deleted", "card_created", "labels", "custom_fields", "file_name", "category",
            "ext", "bytes", "uploaded", "removed_from_card", "status", "local_path",
            "duplicate_of", "sha256", "trello_url", "error"]
    rows = con.execute("""
        SELECT a.id, b.supplier, b.name, l.name, c.name, c.url, c.closed, c.deleted,
               c.date_created, c.labels, c.custom_fields, a.file_name, a.category, a.ext,
               a.bytes, a.date, a.removed, a.status, a.local_path, a.duplicate_of, a.sha256,
               a.url, a.error
        FROM attachments a LEFT JOIN cards c ON c.id=a.card_id
        LEFT JOIN boards b ON b.id=a.board_id LEFT JOIN lists l ON l.id=c.list_id
        ORDER BY b.name, a.date""").fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    shutil.copy(DB_PATH, os.path.join(OUT_DIR, "cad_archive.sqlite"))
    print(f"written {path} ({len(rows)} rows) and a copy of the SQLite catalogue")


# --------------------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("index", help="pull board/card/attachment metadata (no downloads)")
    s.add_argument("--board", help="only boards whose name contains this text")
    s.set_defaults(func=cmd_index)
    sub.add_parser("report", help="summarise the index").set_defaults(func=cmd_report)
    s = sub.add_parser("download", help="download files into ./archive")
    s.add_argument("--board", help="only boards whose name contains this text")
    s.add_argument("--categories", default=",".join(DEFAULT_DOWNLOAD))
    s.add_argument("--workers", type=int, default=6)
    s.add_argument("--limit", type=int, help="stop after N files (for a test run)")
    s.add_argument("--retry-failed", action="store_true")
    s.set_defaults(func=cmd_download)
    sub.add_parser("recategorize", help="re-apply file categories to the catalogue"
                   ).set_defaults(func=cmd_recategorize)
    sub.add_parser("export", help="write catalogue.csv").set_defaults(func=cmd_export)
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
