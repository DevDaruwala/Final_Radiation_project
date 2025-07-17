# Use slim Python as the base
FROM python:3.10-slim

WORKDIR /app

# Install any OS-level deps you need
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy & install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Declare the build‐time arg and promote it to an ENV var
ARG MAPBOX_API_KEY
ENV MAPBOX_API_KEY=${MAPBOX_API_KEY}

# Run Streamlit
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
