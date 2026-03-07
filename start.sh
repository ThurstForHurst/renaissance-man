#!/bin/bash

# Self-Improvement Dashboard - Quick Start Script

echo "🎯 Self-Improvement Dashboard Setup"
echo "===================================="
echo ""

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8 or higher."
    exit 1
fi

echo "✓ Python found: $(python3 --version)"
echo ""

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

echo ""

# Activate and install dependencies
echo "📥 Installing dependencies..."
./venv/bin/pip install -q --upgrade pip
./venv/bin/pip install -q -r requirements.txt

if [ $? -eq 0 ]; then
    echo "✓ Dependencies installed successfully"
else
    echo "❌ Failed to install dependencies"
    exit 1
fi

echo ""

# Create .env if it doesn't exist
if [ ! -f ".env" ]; then
    echo "⚙️  Creating .env file..."
    cp .env.example .env
    echo "✓ Created .env file (edit it to customize settings)"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "To start the app:"
echo "  ./venv/bin/python app/app.py"
echo ""
echo "Then open your browser to: http://localhost:8050"
echo ""
