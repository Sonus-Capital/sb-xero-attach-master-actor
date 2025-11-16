import asyncio
import csv
import io
import json
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


# ---------- main ----------

async def main():
    # Use explicit init/exit (matches docs + avoids Actor.run confusion)
    await Actor.init()
    try:
        actor_input = await Actor.get_input() or {}
        Actor.log.info(f"Actor input keys: {list(actor_input.keys())}")

        # Make is sending: { "json": "<big string>" }
        raw = actor_input.get("json") or actor_input.get("input") or ""
        if not isinstance(raw, str):
            Actor.log.info(f"'json' field is not a string, got {type(raw)}, json-dumping it")
            try:
                raw = json.dumps(raw)
            except Exception:
                raw = str(raw)

        if not raw:
            Actor.log.error("Missing 'json' (or 'input') field in actor input.")
            await Actor.set_value("OUTPUT", {
                "ok": False,
                "stage": "missing_json",
                "error": "Missing 'json' (or 'input') field in actor input.",
                "actor_input_keys": list(actor_input.keys()),
            })
            return

        # 1) Parse outer {"Year": "...", "Links": "..."}
        try:
            payload = json.loads(raw)
        except Exception as e:
            Actor.log.exception("Failed to json.loads() outer json")
            await Actor.set_value("OUTPUT", {
                "ok": False,
                "stage": "parse_outer",
                "error": f"{e}",
                "raw_sample": raw[:500],
            })
            return

        year = str(payload.get("Year") or "")
        links_blob = payload.get("Links") or ""

        if not isinstance(links_blob, str):
            Actor.log.error("Links is not a string in payload.")
            await Actor.set_value("OUTPUT", {
                "ok": False,
                "stage": "links_not_string",
                "error": "Links field is not a string.",
                "payload_sample": json.dumps(payload)[:500],
            })
            return

        if not year or not links_blob.strip():
            Actor.log.error("Year or Links missing/empty after parsing.")
            await Actor.set_value("OUTPUT", {
                "ok": False,
                "stage": "missing_year_or_links",
                "error": "Year or Links missing/empty after parsing.",
                "payload": payload,
            })
            return

        # 2) Turn the Links string into JSON array: "[ {...}, {...}, {...} ]"
        links_json = "[" + links_blob + "]"
        try:
            link_items = json.loads(links_json)
        except Exception as e:
            Actor.log.exception("Failed to parse Links blob into JSON array")
            await Actor.set_value("OUTPUT", {
                "ok": False,
                "stage": "parse_links",
                "error": f"{e}",
                "links_blob_sample": links_blob[:500],
            })
            return

        # 3) Extract TempLink values
        urls = [
            item.get("TempLink")
            for item in link_items
            if isinstance(item, dict) and item.get("TempLink")
        ]
        Actor.log.info(f"Parsed {len(urls)} TempLink URLs from Links")

        if not urls:
            Actor.log.error("No TempLink entries found after parsing.")
            await Actor.set_value("OUTPUT", {
                "ok": False,
                "stage": "no_templinks",
                "error": "No TempLink entries found after parsing.",
                "year": year,
                "link_items": link_items,
            })
            return

        # 4) Download CSVs, merge, classify
        all_rows = []
        for url in urls:
            Actor.log.info(f"Downloading {url}")
            try:
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

        Actor.log.info(f"Total parsed rows from all CSVs: {len(all_rows)}")

        if not all_rows:
            Actor.log.error("No rows parsed from any CSV.")
            await Actor.set_value("OUTPUT", {
                "ok": False,
                "stage": "no_rows",
                "error": "No rows parsed from any CSV.",
                "year": year,
                "urls": urls,
            })
            return

        fieldnames, processed_rows, group_count = merge_and_classify(all_rows)
        Actor.log.info(
            f"After grouping: {group_count} groups, {len(processed_rows)} rows"
        )

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in processed_rows:
            writer.writerow(r)

        master_csv = buf.getvalue()

        filename = f"attach_master_{year}.csv" if year else "attach_master.csv"

        # Save file + structured OUTPUT
        await Actor.set_value(
            filename,
            master_csv,
            content_type="text/csv; charset=utf-8",
        )

        await Actor.set_value("OUTPUT", {
            "ok": True,
            "year": year,
            "rows": len(processed_rows),
            "groups": group_count,
            "csv_key": filename,
        })

        Actor.log.info(
            f"Done. Year={year}, rows={len(processed_rows)}, "
            f"groups={group_count}, file={filename}"
        )

    finally:
        await Actor.exit()


if __name__ == "__main__":
    asyncio.run(main())
