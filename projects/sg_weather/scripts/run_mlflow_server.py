#!/usr/bin/env python3
"""
Simple script to ensure MLflow server is running.
Run this before running any flows that log to MLflow.
"""

import subprocess
import time
import sys
import os
from pathlib import Path


def start_mlflow_server():
    print("Starting MLflow server...")

    # Create mlflow directory if it doesn't exist
    mlflow_dir = Path.home() / "mlflow"
    mlflow_dir.mkdir(exist_ok=True)

    try:
        # Check if server is already running
        result = subprocess.run(
            ["pgrep", "-f", "mlflow server"], capture_output=True, text=True
        )

        if result.stdout.strip():
            print("MLflow server is already running!")
            print("To access the UI, visit: http://localhost:5000")
            return True

        # Start the server with sqlite backend
        db_path = mlflow_dir / "mlflow.db"
        artifacts_path = mlflow_dir / "artifacts"
        artifacts_path.mkdir(exist_ok=True)

        cmd = [
            "mlflow",
            "server",
            "--backend-store-uri",
            f"sqlite:///{db_path}",
            "--default-artifact-root",
            f"file://{artifacts_path}",
            "--host",
            "0.0.0.0",
            "--port",
            "5000",
        ]

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Wait for server to start
        time.sleep(5)

        # Check if server started successfully
        if process.poll() is None:  # Process is still running
            print("MLflow server started successfully!")
            print("To access the UI, visit: http://localhost:5000")
            return True
        else:
            stdout, stderr = process.communicate()
            print(f"Failed to start MLflow server: {stderr}")
            return False

    except Exception as e:
        print(f"Error starting MLflow server: {e}")
        return False


if __name__ == "__main__":
    success = start_mlflow_server()
    sys.exit(0 if success else 1)
