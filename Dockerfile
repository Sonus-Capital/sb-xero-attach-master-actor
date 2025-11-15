FROM apify/actor-python:3.13

# Copy everything
COPY . ./

# Install dependencies if present
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Run your actor
CMD ["python", "-m", "src.main"]
