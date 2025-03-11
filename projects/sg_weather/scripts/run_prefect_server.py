#!/usr/bin/env python3
"""
Script to start or stop the Prefect server.
Usage: python run_prefect_server.py [start|stop]
"""

import subprocess
import time
import sys
import signal
import os


def start_prefect_server():
    print("Starting Prefect server...")
    try:
        # Check if server is already running
        result = subprocess.run(
            ["prefect", "server", "status"], capture_output=True, text=True
        )

        if "is running" in result.stdout:
            print("Prefect server is already running!")
            print("To access the UI, visit: http://localhost:4200")
            return True

        # Start the server
        process = subprocess.Popen(
            ["prefect", "server", "start"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Wait for server to start
        time.sleep(5)

        # Check if server started successfully
        if process.poll() is None:  # Process is still running
            print("Prefect server started successfully!")
            print("To access the UI, visit: http://localhost:4200")
            return True
        else:
            stdout, stderr = process.communicate()
            print(f"Failed to start Prefect server: {stderr}")
            return False

    except Exception as e:
        print(f"Error starting Prefect server: {e}")
        return False


def stop_prefect_server():
    print("Stopping Prefect server...")
    try:
        # Find Prefect server processes
        result = subprocess.run(
            ["pgrep", "-f", "prefect server start"], capture_output=True, text=True
        )

        pids = result.stdout.strip().split("\n")
        pids = [pid for pid in pids if pid]

        if not pids:
            print("No running Prefect server found.")
            return True

        # Kill each process
        for pid in pids:
            try:
                os.kill(int(pid), signal.SIGTERM)
                print(f"Sent termination signal to Prefect server (PID: {pid})")
            except ProcessLookupError:
                print(f"Process {pid} not found")
            except Exception as e:
                print(f"Error killing process {pid}: {e}")

        # Verify all processes are stopped
        time.sleep(2)
        result = subprocess.run(
            ["pgrep", "-f", "prefect server start"], capture_output=True, text=True
        )

        if result.stdout.strip():
            print("Warning: Some Prefect processes may still be running.")
            return False
        else:
            print("Prefect server stopped successfully.")
            return True

    except Exception as e:
        print(f"Error stopping Prefect server: {e}")
        return False


if __name__ == "__main__":
    action = "start"
    if len(sys.argv) > 1:
        action = sys.argv[1].lower()

    if action == "start":
        success = start_prefect_server()
    elif action == "stop":
        success = stop_prefect_server()
    else:
        print(f"Unknown action: {action}")
        print("Usage: python run_prefect_server.py [start|stop]")
        success = False

    sys.exit(0 if success else 1)
