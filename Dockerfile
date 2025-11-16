FROM apify/actor-python:3.13

# Copy everything (including .actor/)
COPY . ./

# Install deps if present
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Run our actual main.py inside .actor/src
CMD ["python", ".actor/src/main.py"]
