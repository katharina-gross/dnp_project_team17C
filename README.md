# Distributed Log Analysis using a Custom MapReduce Framework

## Overview

This project implements a mini MapReduce framework in Python for distributed log analysis.

The system processes HTTP log files, splits them into chunks, distributes map tasks across multiple worker processes, groups intermediate results, and reduces them into final aggregated output.

This implementation is based on **Variant C** of the project:

**Analyze error frequency over time with time-bucketed reducers**

The system:
- parses HTTP log files
- detects error responses
- groups them by minute
- separates `4xx` and `5xx` errors
- aggregates final counts
- supports benchmarking, validation, and simulated failure recovery

---

## Features

- Custom MapReduce workflow
- Log splitting into chunks
- Parallel processing with `multiprocessing`
- Error aggregation by time bucket
- Separate counting of `4xx` and `5xx` errors
- Validation using known test logs
- Benchmarking with different chunk sizes and worker counts
- Simulated worker failure and recovery
- Unit tests for core components

---

## Installation

### Requirements

- Python 3.11 or higher
- pip

---

### Setup

1. Clone the repository:

```bash
git clone https://github.com/katharina-gross/dnp_project_team17C.git
cd dnp_project_team17C

```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

### How to run

### Run all system

```bash
python -m main --mode all
```

### Run specific modes

```bash
python -m main --mode run         # main pipeline
python -m main --mode validation  # validation
python -m main --mode benchmark   # performance benchmark
python -m main --mode failure     # failure recovery demo
python -m main --mode test        # run tests
```

### Output Files

After running the system, results are stored in results/final:

### benchmark.json
Main output file containing:
- pipeline result (error counts per time bucket)
- execution statistics
- validation status
- benchmark results

### benchmark_plot.png
Graph showing performance depending on:
- chunk size
- number of workers

### recovery_log.txt
Contains:
- simulated failure details
- recovery steps
- final result after recovery


## Input Format

Each log line must follow this format:

```text
IP - - [DD/Mon/YYYY:HH:MM:SS] "METHOD /path HTTP/1.1" STATUS SIZE
```

Example:

```text
127.0.0.1 - - [02/Apr/2026:10:01:15] "GET /login HTTP/1.1" 404 321
```

Invalid or malformed lines are ignored.

## How It Works

The system follows the MapReduce pipeline:

1. **Split**  
   The log file is divided into chunks.

2. **Map**  
   Each worker processes log lines and emits:

```text
(time_bucket + error_type) → 1
```

3. **Shuffle**  
Results are grouped by key.

4. **Reduce**  
Values are aggregated into final counts.


