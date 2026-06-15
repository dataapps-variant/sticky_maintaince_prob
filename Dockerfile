# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy code and install dependencies
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the maintenance script (Cloud Run Job: runs to completion, then exits)
CMD ["python", "main.py"]
