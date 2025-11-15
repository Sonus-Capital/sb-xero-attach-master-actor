from apify import Actor

# Minimal, always-passing test entrypoint for Apify's image self-check.
# Our real logic lives in src/__main__.py.

async def main():
    async with Actor:
        # Just log something and exit successfully
        await Actor.log("Self-test main.py: OK")
        # Optionally set a tiny output so the test has something to see
        await Actor.set_output({"self_test": "ok"})


if __name__ == "__main__":
    # Allow running this module directly, though Apify mainly uses __main__.py
    Actor.run(main)
