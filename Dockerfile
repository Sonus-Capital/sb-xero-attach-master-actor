FROM apify/actor-python:3.13

# Copy the whole repo (including src/, .actor/, etc.)
COPY . ./

# Install dependencies if any
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Run the src package (this calls src/__main__.py)
CMD ["python", "-m", "src"]
