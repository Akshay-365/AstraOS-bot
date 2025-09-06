# Dockerfile
FROM python:3.9-slim

# Set a working directory
# WORKDIR /app

# Copy dependency definitions and install them
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Expose the port that uvicorn will listen on (Hugging Face Spaces uses port 7860)
EXPOSE 7860

# Start the FastAPI app with uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]