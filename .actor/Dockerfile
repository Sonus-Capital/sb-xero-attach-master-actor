FROM apify/actor-python:3.13

# Copy everything into the image
COPY . ./

# Install Python dependencies (none yet, but keep the file for later)
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Default command – run our src package
CMD ["python", "-m", "src"]
