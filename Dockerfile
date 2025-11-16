FROM apify/actor-python:3.13

# Copy the whole repo (including src/, .actor/, etc.)
COPY . ./

# Install Python dependencies if you ever add requirements.txt
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Run our main module explicitly
CMD ["python", "-m", "src.main"]
