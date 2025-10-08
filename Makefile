# Makefile to simplify common commands for macOS/Linux.
# On Windows, you can run the python commands directly instead of using make.

.PHONY: venv data baseline optimized report clean

venv:
# Create a virtual environment and install requirements
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

data:
# Generate dummy data with skew + many small files
	. .venv/bin/activate && python src/jobs/generate_data.py

baseline:
# Run the intentionally slow/naive job and log its runtime
	. .venv/bin/activate && python src/jobs/job_baseline.py

optimized:
# Run the optimized job and log its runtime
	. .venv/bin/activate && python src/jobs/job_optimized.py

report:
# Build p50/p95 summary and a before→after chart
	. .venv/bin/activate && python src/utils/metrics.py report

clean:
# Remove generated outputs and metrics
	rm -rf data/bronze/* data/silver/* data/gold/* ops/job_runs.csv ops/report.md ops/p95_chart.png
