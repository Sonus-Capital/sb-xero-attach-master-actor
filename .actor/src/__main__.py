import json

# Apify's Python-code template will provide a global `input` dict
# and expect us to set a global `output` dict.

def main():
    global input, output  # provided by Apify

    actor_input = input or {}

    # For now we just read a 'json' field and echo it back parsed.
    raw = actor_input.get("json") or actor_input.get("input") or ""
    if not raw:
        output = {
            "ok": False,
            "error": "Missing 'json' (or 'input') field in actor input.",
            "actor_input": actor_input,
        }
        return

    try:
        parsed = json.loads(raw)
    except Exception as e:
        output = {
            "ok": False,
            "error": f"Failed to json.loads() the provided string: {e}",
            "raw_sample": raw[:200],
        }
        return

    # Success – just echo it back for now
    output = {
        "ok": True,
        "message": "Actor ran successfully and parsed your JSON.",
        "parsed": parsed,
    }


if __name__ == "__main__":
    # Apify's Python runtime calls `python -m src`,
    # which executes this block.
    main()
