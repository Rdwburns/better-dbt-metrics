#!/usr/bin/env python3
"""
Simple test to check if compilation works
"""

import subprocess
import sys
import os

# Install in development mode
print("Installing package in development mode...")
subprocess.run([sys.executable, "-m", "pip", "install", "-e", "."], check=True)

# Now test compilation
print("\nTesting compilation...")
result = subprocess.run([
    sys.executable, "-m", "cli.main", "compile",
    "--input-dir", "examples/advanced/",
    "--output-dir", "test_output/",
    "--no-validate"
], capture_output=True, text=True)

print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print("Return code:", result.returncode)