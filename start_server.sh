#!/bin/bash

# Nuvyn Executor Script - Start API Server
# This script starts the HTTP API server for curl-based job execution

echo "🚀 Starting Nuvyn Executor API Server..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed or not in PATH"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "executor/api_server.py" ]; then
    echo "❌ Please run this script from the nuvyn-installer directory"
    echo "   Current directory: $(pwd)"
    exit 1
fi

# Set default values
HOST=${HOST:-"0.0.0.0"}
PORT=${PORT:-8080}
LOG_LEVEL=${LOG_LEVEL:-"INFO"}

echo "📋 Configuration:"
echo "   Host: $HOST"
echo "   Port: $PORT"
echo "   Log Level: $LOG_LEVEL"
echo ""

# Install dependencies if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "📦 Installing dependencies..."
    pip install -r requirements.txt
    echo ""
fi

# Start the API server
echo "🌐 Starting API server on http://$HOST:$PORT"
echo "📖 API documentation available at http://$HOST:$PORT/info"
echo "💡 Use curl_examples.md for usage examples"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

python3 executor/api_server.py --host "$HOST" --port "$PORT" --log-level "$LOG_LEVEL"
