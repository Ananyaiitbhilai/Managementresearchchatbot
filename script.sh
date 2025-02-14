#!/bin/bash
# Usage: ./run_pipeline.sh <input_directory>

# Configuration
VENV_NAME=".rag_venv"
REQUIREMENTS="requirements.txt"
OUTPUT_BASE="processed_outputs"

# Initialize logging
LOG_DIR="pipeline_logs"
mkdir -p "$LOG_DIR"

# Check input parameter
if [ -z "$1" ]; then
    echo "Error: Please provide input directory"
    echo "Usage: ./run_pipeline.sh <input_directory>"
    exit 1
fi
INPUT_DIR="$1"

# Create virtual environment
echo "======= Setting up environment ======="
python3 -m venv "$VENV_NAME"
source "$VENV_NAME/bin/activate"

# Install requirements
echo "======= Installing dependencies ======="
pip install -r "$REQUIREMENTS" > "$LOG_DIR/install.log" 2>&1

# Create output directory structure
mkdir -p "$OUTPUT_BASE"

# Pipeline execution
echo "======= Starting processing pipeline ======="

# Step 1: XML to JSON conversion
echo "======= Step1 - xml2json.py running (see $LOG_DIR/xml2json.log) ======="
python xml2json.py \
    --input "$INPUT_DIR" \
    --output "$OUTPUT_BASE/json_output" \
    > "$LOG_DIR/xml2json.log" 2>&1

if [ $? -ne 0 ]; then
    echo "Error in Step1! Check $LOG_DIR/xml2json.log"
    exit 1
fi
echo "==== Step1 completed - json_output created ===="
echo

# Step 2: Paper segregation
echo "======= Step2 - segregate.py running (see $LOG_DIR/segregate.log) ======="
python segregate.py \
    --input "$OUTPUT_BASE/json_output" \
    --output "$OUTPUT_BASE/management_full_papers" \
    > "$LOG_DIR/segregate.log" 2>&1

if [ $? -ne 0 ]; then
    echo "Error in Step2! Check $LOG_DIR/segregate.log"
    exit 1
fi
echo "==== Step2 completed - management_full_papers created ===="
echo

# Step 3: Attribute selection
echo "======= Step3 - selective.py running (see $LOG_DIR/selective.log) ======="
python selective.py \
    --input "$OUTPUT_BASE/management_full_papers" \
    --output "$OUTPUT_BASE/management_full_papers_selected_attributes" \
    > "$LOG_DIR/selective.log" 2>&1

if [ $? -ne 0 ]; then
    echo "Error in Step3! Check $LOG_DIR/selective.log"
    exit 1
fi
echo "==== Step3 completed - management_full_papers_selected_attributes created ===="
echo

# Cleanup
deactivate
echo "======= Pipeline completed successfully ======="
echo "Final output available at: management_full_papers_selected_attributes"
