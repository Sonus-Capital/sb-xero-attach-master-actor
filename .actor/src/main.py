import os
import json
import csv
import io
import re
import urllib.request

from apify import Actor

# ---------- helpers ----------

def norm(s):
    if s is None:
        return ""
    return str(s).strip()


ATTACH_PREFIX_RE = re.compile(r"^attach_invoices_\d{4}\.csv\s+", re.IGNORECASE)


def clean_prefix(v):
    return ATTACH_PREFIX_RE.sub("", norm(v))


def get_type(row):
    return norm(row.get("Xero type") or row.get("Type"))


def get_year(row):
    return norm(row.get("Xero year") or row.get("Year"))


def build_key(row):
    inv = clean_prefix(row.get("Invoice ID"))
    line = clean_prefix(row.get("Line item ID"))
    t = get_type(row)
    y = get_year(row)

    # Full line-level key
    if inv and line and t and y:
        return f"{inv}::{line}::{t}::{y}"

    # Invoice-level fallback
    if inv and t and y:
        return f"INV::{inv}::{t}::{y}"

    fname = norm(
        row.get("File name")
        or row.get("Attachment file name dropbox")
        or row.get("Attachment file name xero")
    )
    ch = norm(row.get("Content hash"))

    if ch:
        return f"HASH::{ch}"
    if fname and inv:
        return f"INVFN::{inv}::{fname}"

    return ""


def is_file_row(row):
    if norm(row.get("File tag")):
        return True
    if norm(row.get("Drop box file name")):
        return True
    if norm(row.get("Path lower")) and not norm(row.get("Entity code")):
        return True
    return False


def is_invoice_row(row):
    if norm(row.get("Entity code")):
        return True
    if norm(row.get("Invoice reference")):
        return True
    if norm(row.get("Line account code")):
        return True
    if norm(row.get("Invoice ID")) and norm(row.get("Xero type")):
        return True
    return False


def merge_and_classify(rows):
    if not rows:
        return [], [], 0

    # union of headers
    fieldnames = list(rows[0].keys())
    seen = set(fieldnames)
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    # ensure our columns
    for col in ["Master attachment key", "Category_Bucket", "Likely_Related"]:
        if col not in fieldnames:
            fieldnames.append(col)

    groups = {}
    for idx, row in enumerate(rows):
        gk = norm(row.get("Master attachment key")) or build_key(row)
        if not gk:
            gk = f"ROW::{idx}"

        if gk not in groups:
            groups[gk] = {"all": [], "inv": [], "file": []}

        groups[gk]["all"].append(idx)
        if is_invoice_row(row):
            groups[gk]["inv"].append(idx)
        if is_file_row(row):
            groups[gk]["file"].append(idx)

        row["Master attachment key"] = gk

    # assign buckets
    for gk, meta in groups.items():
        inv = meta["inv"]
        file = meta["file"]
        has_inv = len(inv) > 0
        has_file = len(file) > 0

        if has_inv and has_file:
            bucket = "Invoice+File"
            if len(file) > 1:
                bucket = "Invoice+MultiFile"
            for i in meta["all"]:
                r = rows[i]
                if not norm(r.get("Category_Bucket")):
                    r["Category_Bucket"] = bucket
                r["Likely_Related"] = "Y"

        elif has_inv and not has_file:
            for i in inv:
                r = rows[i]
                if not norm(r.get("Category_Bucket")):
                    r["Category_Bucket"] = "Invoice_Only"

        elif has_file and not has_inv:
            bucket = "Orphan_File_Group" if len(file) > 1 else "Orphan_File"
            for i in file:
                r = rows[i]
                if not norm(r.get("Category_Bucket")):
                    r["Category_Bucket"] = bucket
                r["Likely_Related"] = "Y"

        else:
            for i in meta["all"]:
                r = rows[i]
                if not norm(r.get("Category_Bucket")):
                    r["Category_Bucket"] = "Orphan_Unknown"

    return fieldnames, rows, len(groups)


# ---------- Dropbox upload helper ----------

async def upload_to_dropbox(filename: str, csv_text: str, year: str | None = None):
    """
    Upload the CSV to Dropbox and (optionally) apply a tag.

    Environment variables (set on the Actor):
      - DROPBOX_TOKEN   (required) OAuth2 Bearer token
      - DROPBOX_ROOT    (optional) folder path, default "/Sona/Milseain & Progeny Invoices/_outputs"
      - DROPBOX_TAG     (optional) tag text, e.g. "sbpage"
    """
    token = os.getenv("DROPBOX_TOKEN")
    root = os.getenv("DROPBOX_ROOT", "/Sona/Milseain & Progeny Invoices/_outputs")
    tag = os.getenv("DROPBOX_TAG", "sbpage")

    if not token:
        Actor.log.warning("DROPBOX_TOKEN not set; skipping Dropbox upload.")
        return None

    path = f"{root.rstrip('/')}/{filename}"
    data = csv_text.encode("utf-8")

    # /2/files/upload
    api_arg = json.dumps({
        "path": path,
        "mode": "overwrite",
        "mute": False,
        "autorename": False,
    })

    req = urllib.request.Request(
        "https://content.dropboxapi.com/2/files/upload",
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": api_arg,
        },
    )

    try:
        with urllib.request.urlopen(req) as resp:
            resp_data = resp.read()
        info = json.loads(resp_data.decode("utf-8"))
        path_lower = info.get("path_lower") or path.lower()
        Actor.log.info(f"Uploaded CSV to Dropbox at {path_lower}")
    except Exception as e:
        Actor.log.error(f"Dropbox upload failed: {e}")
        return None

    # Optionally add a tag (matches your existing tagging approach)
    if tag:
        try:
            body = json.dumps({
                "path": path_lower,
                "tag_text": tag,
            }).encode("utf-8")

            tag_req = urllib.request.Request(
                "https://api.dropboxapi.com/2/files/tags/add",
                data=body,
                method="POST",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
            with urllib.request.urlopen(tag_req) as resp:
                _ = resp.read()
            Actor.log.info(f"Applied Dropbox tag '{tag}' to {path_lower}")
        except Exception as e:
            Actor.log.error(f"Dropbox tag add failed: {e}")

    return path


# ---------- Apify entrypoint ----------

async def main():
    async with Actor:
        actor_input = await Actor.get_input() or {}
        Actor.log.info(f"Actor input keys: {list(actor_input.keys())}")

        # Make is sending: { "json": "<big string>" }
        raw = actor_input.get("json") or ""
        if not raw:
            Actor.log.error("Missing 'json' field in actor input.")
            return

        Actor.log.info(f"Raw 'json' sample: {raw[:200]}")

        # 1) Parse outer {"Year": "...", "Links": "..."}
        try:
            payload = json.loads(raw)
        except Exception as e:
            Actor.log.error(f"Failed to json.loads() outer json: {e}")
            return

        Actor.log.info(
            f"Parsed payload type={type(payload)}"
            f", repr={repr(str(payload))[:200]}"
        )

        year = str(payload.get("Year") or "").strip()
        links_blob = payload.get("Links") or ""

        if not year or not links_blob.strip():
            Actor.log.error("Year or Links missing/empty after parsing.")
            return

        # 2) Turn the Links string into JSON array: "[ {...}, {...}, {...} ]"
        links_json = "[" + links_blob + "]"
        try:
            link_items = json.loads(links_json)
        except Exception as e:
            Actor.log.error(
                f"Failed to parse Links blob into JSON array: {e} "
                f"sample={links_blob[:200]}"
            )
            return

        urls = [item.get("TempLink") for item in link_items if item.get("TempLink")]
        Actor.log.info(f"Parsed {len(urls)} TempLink URLs from Links")

        if not urls:
            Actor.log.error("No TempLink entries found after parsing.")
            return

        # 3) Download CSVs and merge
        all_rows = []
        for url in urls:
            try:
                Actor.log.info(f"Downloading {url}")
                with urllib.request.urlopen(url) as resp:
                    csv_bytes = resp.read()
            except Exception as e:
                Actor.log.error(f"Failed to download {url}: {e}")
                continue

            csv_text = csv_bytes.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                row = dict(row)
                row["__source_year"] = year
                row["__source_url"] = url
                all_rows.append(row)

        if not all_rows:
            Actor.log.error("No rows parsed from any CSV.")
            return

        Actor.log.info(f"Total parsed rows from all CSVs: {len(all_rows)}")

        fieldnames, processed_rows, group_count = merge_and_classify(all_rows)
        Actor.log.info(
            f"After grouping: {group_count} groups, {len(processed_rows)} rows"
        )

        # 4) Build CSV in-memory
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in processed_rows:
            writer.writerow(r)

        master_csv = buf.getvalue()

        filename = f"attach_master_{year}.csv" if year else "attach_master.csv"

        # Save into default KV store (Apify)
        await Actor.set_value(
            filename,
            master_csv,
            content_type="text/csv; charset=utf-8",
        )

        # Also upload directly to Dropbox
        dropbox_path = await upload_to_dropbox(filename, master_csv, year)

        # Structured output for debugging / Make, etc.
        await Actor.set_output({
            "ok": True,
            "year": year,
            "rows": len(processed_rows),
            "groups": group_count,
            "kv_key": filename,
            "dropbox_path": dropbox_path,
        })

        Actor.log.info(
            f"Done. Year={year}, rows={len(processed_rows)}, "
            f"groups={group_count}, kv_key={filename}, "
            f"dropbox_path={dropbox_path}"
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
