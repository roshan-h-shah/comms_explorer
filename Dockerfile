# Use official slim Python image
FROM python:3.11-slim

# Prevent Python from writing pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl wget gnupg unzip git \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 libxss1 libasound2 \
    libgbm-dev libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js (needed for playwright CLI to work)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Install Playwright + Chromium
RUN pip install playwright
RUN playwright install chromium

# Copy all app files
COPY . /app
WORKDIR /app

# Expose port
EXPOSE 8000

# Start FastAPI app with uvicorn
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
