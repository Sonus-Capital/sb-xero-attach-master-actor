async def main():
    """
    Apify Python Actor entrypoint.
    - Expects input like: { "json": "<string from Make>" }
    - That string is: {"Year":"2016","Links":"{\"TempLink\":\"https://...\"}, {\"TempLink\":\"https://...\"}, ..."}
    - Downloads CSVs, merges + classifies, writes master CSV to key-value store and output.
    """
    async with Actor:
        # 0) Log raw actor input for debugging
        actor_input = await Actor.get_input() or {}
        await Actor.log(f"Actor input received: keys={list(actor_input.keys())}")

        # Your Year+Links JSON is in the "json" field (what Make sends)
        raw = actor_input.get("json") or actor_input.get("input") or ""
        await Actor.log(f"Raw payload length: {len(raw)}")

        if not raw:
            await Actor.set_output({
                "ok": False,
                "error": "Missing 'json' (or 'input') field in actor input.",
                "actor_input": actor_input,
            })
            await Actor.log("Exiting: missing json/input field.")
            return

        # 1) Parse outer {"Year": "...", "Links": "..."}
        try:
            payload = json.loads(raw)
        except Exception as e:
            await Actor.log(f"JSON decode error on outer payload: {e}")
            await Actor.set_output({
                "ok": False,
                "error": f"Failed to json.loads() outer json: {e}",
                "raw_sample": raw[:200],
            })
            return

        year = str(payload.get("Year") or "")
        links_blob = payload.get("Links") or ""
        await Actor.log(f"Parsed year={year!r}, links_blob_len={len(links_blob)}")

        if not year or not links_blob.strip():
            await Actor.set_output({
                "ok": False,
                "error": "Year or Links missing/empty after parsing.",
                "payload": payload,
            })
            await Actor.log("Exiting: year or links_blob empty.")
            return

        # 2) Turn the Links string into a proper JSON array: "[ {...}, {...}, {...} ]"
        links_json = "[" + links_blob + "]"
        await Actor.log(f"links_json preview: {links_json[:200]}")
        try:
            link_items = json.loads(links_json)
        except Exception as e:
            await Actor.log(f"JSON decode error on Links blob: {e}")
            await Actor.set_output({
                "ok": False,
                "error": f"Failed to parse Links blob into JSON array: {e}",
                "links_blob_sample": links_blob[:200],
            })
            return

        # 3) Extract TempLink values
        urls = [item.get("TempLink") for item in link_items if item.get("TempLink")]
        await Actor.log(f"Extracted {len(urls)} TempLink URLs")

        if not urls:
            await Actor.set_output({
                "ok": False,
                "error": "No TempLink entries found after parsing.",
                "year": year,
                "link_items": link_items,
            })
            await Actor.log("Exiting: no TempLink entries.")
            return

        # 4) Download CSVs, merge, classify
        all_rows = []
        for url in urls:
            try:
                await Actor.log(f"Downloading CSV from: {url}")
                with urllib.request.urlopen(url) as resp:
                    csv_bytes = resp.read()
            except Exception as e:
                await Actor.log(f"Failed to download {url}: {e}")
                continue

            csv_text = csv_bytes.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(csv_text))
            count_for_url = 0
            for row in reader:
                row = dict(row)
                row["__source_year"] = year
                row["__source_url"] = url
                all_rows.append(row)
                count_for_url += 1
            await Actor.log(f"Parsed {count_for_url} rows from {url}")

        await Actor.log(f"Total rows parsed from all CSVs: {len(all_rows)}")

        if not all_rows:
            await Actor.set_output({
                "ok": False,
                "error": "No rows parsed from any CSV.",
                "year": year,
            })
            await Actor.log("Exiting: no rows parsed from any CSV.")
            return

        fieldnames, processed_rows, group_count = merge_and_classify(all_rows)
        await Actor.log(
            f"merge_and_classify finished: rows={len(processed_rows)}, groups={group_count}"
        )

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in processed_rows:
            writer.writerow(r)

        master_csv = buf.getvalue()
        kv_key = f"attach_master_{year}.csv"

        await Actor.set_value(kv_key, master_csv, content_type="text/csv; charset=utf-8")
        await Actor.log(f"Saved CSV to key-value store under key: {kv_key}")

        await Actor.set_output({
            "ok": True,
            "year": year,
            "rows": len(processed_rows),
            "groups": group_count,
            "kv_key": kv_key,
            "master_csv": master_csv,
        })
        await Actor.log("Actor finished successfully.")
