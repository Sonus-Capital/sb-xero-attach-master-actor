FROM apify/actor-python:3.13

# Copy the whole repo (including src/, .actor/, etc.)
COPY . ./

# Dependencies (you probably don't even need requirements.txt, but this is safe)
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Run the src package, which will call our __main__.py
CMD ["python", "-m", "src"]
