from apify import Actor
import json
import csv
import io
import urllib.request

# leave all your existing helper functions above this as-is:
# norm, clean_prefix, get_type, get_year, build_key,
# is_file_row, is_invoice_row, merge_and_classify


async def main():
    """
    Apify Python Actor entrypoint.

    Expects actor input like:
      {
        "json": "{\"Year\":\"2016\",\"Links\":\"{\\\"TempLink\\\":\\\"https://.../file\\\"}, {\\\"TempLink\\\":\\\"https://.../file\\\"}, {\\\"TempLink\\\":\\\"https://.../file\\\"}\"}"
      }

    - Parses that string
    - Extracts TempLinks
    - Downloads all CSVs
    - Merges + classifies rows
    - Returns master_csv and stores it in KV store.
    """
    async with Actor:
        actor_input = await Actor.get_input() or {}

        # Your Create JSON step now outputs a top-level "json" field
        raw = actor_input.get("json") or actor_input.get("input") or ""
        if not raw:
            await Actor.set_output({
                "ok": False,
                "error": "Missing 'json' (or 'input') field in actor input.",
                "actor_input": actor_input,
            })
            return

        # 1) Parse outer {"Year": "...", "Links": "..."}
        try:
            payload = json.loads(raw)
        except Exception as e:
            await Actor.set_output({
                "ok": False,
                "error": f"Failed to json.loads() outer json: {e}",
                "raw_sample": raw[:200],
            })
            return

        year = str(payload.get("Year") or "")
        links_blob = payload.get("Links") or ""

        if not year or not links_blob.strip():
            await Actor.set_output({
                "ok": False,
                "error": "Year or Links missing/empty after parsing.",
                "payload": payload,
            })
            return

        # 2) Turn the Links string into a proper JSON array: "[ {...}, {...}, {...} ]"
        links_json = "[" + links_blob + "]"
        try:
            link_items = json.loads(links_json)
        except Exception as e:
            await Actor.set_output({
                "ok": False,
                "error": f"Failed to parse Links blob into JSON array: {e}",
                "links_blob_sample": links_blob[:200],
            })
            return

        # 3) Extract TempLink values
        urls = [item.get("TempLink") for item in link_items if item.get("TempLink")]
        if not urls:
            await Actor.set_output({
                "ok": False,
                "error": "No TempLink entries found after parsing.",
                "year": year,
                "link_items": link_items,
            })
            return

        await Actor.log(f"Year {year}: downloading {len(urls)} CSV(s) from Dropbox.")

        # 4) Download CSVs, merge, classify
        all_rows = []
        for url in urls:
            try:
                with urllib.request.urlopen(url) as resp:
                    csv_bytes = resp.read()
            except Exception as e:
                # Log and move on; don't kill the whole run for one bad link
                await Actor.log(f"Failed to download {url}: {e}")
                continue

            csv_text = csv_bytes.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                r = dict(row)
                r["__source_year"] = year
                r["__source_url"] = url
                all_rows.append(r)

        if not all_rows:
            await Actor.set_output({
                "ok": False,
                "error": "No rows parsed from any CSV.",
                "year": year,
            })
            return

        fieldnames, processed_rows, group_count = merge_and_classify(all_rows)

        # 5) Build the master CSV
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in processed_rows:
            writer.writerow(r)

        master_csv = buf.getvalue()

        # Save to KV store as file for easy download from Apify UI
        kv_key = f"attach_master_{year}.csv"
        await Actor.set_value(kv_key, master_csv, content_type="text/csv; charset=utf-8")

        # Also return JSON so you can see stats + inline CSV (if you want)
        await Actor.set_output({
            "ok": True,
            "year": year,
            "rows": len(processed_rows),
            "groups": group_count,
            "kv_key": kv_key,
            # comment this out if the CSV is huge and you only want the kv_key
            "master_csv": master_csv,
        })
