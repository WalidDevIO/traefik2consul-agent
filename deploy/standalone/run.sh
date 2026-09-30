#!/usr/bin/env bash
set -e

# Go to repository root
cd "$(dirname "$0")/../../"

echo "=== Consul Aggregator Standalone Runner ==="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is required but not installed."
    exit 1
fi

# 1. Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "=> Creating virtual environment in ./venv..."
    python3 -m venv venv
fi

# 2. Activate virtual environment
source venv/bin/activate

# 3. Install dependencies
echo "=> Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

# 4. Create default config if missing
if [ ! -f ".env" ]; then
    if [ -f ".env.template" ]; then
        echo "=> No .env found. Creating one from .env.template..."
        cp .env.template .env
        echo "   (You might want to stop the script and edit .env to suit your needs)"
        sleep 2
    else
        echo "Error: Neither .env nor .env.template exist."
        exit 1
    fi
fi

# 5. Export variables from .env
echo "=> Loading configuration from .env..."
set -a
source .env
set +a

# 6. Run the agent
echo "=> Starting Consul Aggregator (Mode: ${MODE:-cluster})..."
echo "   Dashboard should be accessible at http://localhost:${API_PORT:-8099} (if enabled)"
echo "---------------------------------------------------"

python3 -m consul_aggregator
