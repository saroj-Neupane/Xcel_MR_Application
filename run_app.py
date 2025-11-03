#!/usr/bin/env python3
"""
Xcel MakeReady Sheet QC App Launcher
====================================

This script launches the Xcel MakeReady Sheet QC Application.
It sets up the Python path and starts the main application.

Usage:
    python run_app.py

Requirements:
    - Python 3.7+
    - All dependencies listed in requirements.txt
"""

import sys
import os
from pathlib import Path

def main():
    """Main entry point for the application launcher"""
    
    # Get the directory where this script is located
    script_dir = Path(__file__).parent.absolute()
    
    # Add the src directory to Python path
    src_dir = script_dir / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))
    else:
        print(f"Error: Source directory not found at {src_dir}")
        print("Please ensure you're running this script from the project root directory.")
        sys.exit(1)
    
    # Change to the script directory to ensure relative paths work correctly
    os.chdir(script_dir)
    
    try:
        # Import and run the main application
        from main import main
        print("Starting Xcel MakeReady Sheet QC App...")
        main()
    except ImportError as e:
        print(f"Error importing application modules: {e}")
        print("Please ensure all dependencies are installed:")
        print("  pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"Error starting application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
