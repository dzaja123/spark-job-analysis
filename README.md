# Spark Job Analysis

This project is designed to perform job analysis using Apache Spark with Docker. The analysis is based on a dataset downloaded from Kaggle, and the entire environment is containerized using Docker.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Setup Kaggle API](#setup-kaggle-api)
- [Docker Setup](#docker-setup)
- [Running the Spark Job](#running-the-spark-job)
- [Project Structure](#project-structure)
- [UDF performance comment](#udf-performance-comment)

## Prerequisites
Make sure you have the following software installed on your machine:
- Docker: [Download and Install Docker](https://docs.docker.com/get-docker/)
- Python 3.8 or higher
- A Kaggle account: [Sign up for Kaggle](https://www.kaggle.com/)

## Setup Kaggle API
To download the dataset from Kaggle, you'll need to set up the Kaggle API on your machine.

1. **Generate Kaggle API Token**:
   - Log in to your Kaggle account.
   - Go to **Account** (https://www.kaggle.com/account).
   - Scroll down to the **API** section and click on **Create New API Token**.
   - This will download a `kaggle.json` file containing your API credentials.

2. **Place `kaggle.json` File**:
   - Move the `kaggle.json` file to a secure location. For example:
     - On Windows: `C:\Users\<YourUsername>\.kaggle\kaggle.json`
     - On macOS/Linux: `~/.kaggle/kaggle.json`

3. **Set Up Environment Variable**:
   - Make sure that the `KAGGLE_CONFIG_DIR` environment variable is set to the path where `kaggle.json` is located. Example for Windows:
     ```bash
     set KAGGLE_CONFIG_DIR=C:\Users\<YourUsername>\.kaggle
     ```

   - For macOS/Linux:
     ```bash
     export KAGGLE_CONFIG_DIR=~/.kaggle
     ```

## Docker Setup

1. **Clone the Repository**:
   Clone this repository to your local machine using the following command:
   ```bash
   git clone https://github.com/dzaja123/spark-job-analysis.git
   ```

   ```bash
   cd spark-job-analysis
   ```

   Build the Docker Image: Build the Docker image that will run the Spark job:
   ```bash
   docker-compose build
   ```

   Start the Docker Container: Use Docker Compose to start the Spark services:
   ```bash
   docker-compose up -d
   ```

   Run the Analysis Script: Once the containers are up, run the Spark analysis job:
   ```bash
   docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/spark_app/job_analysis.py
   ```
   Check Results: The analysis results will be saved in the specified output path within the container or to a local directory if specified in the script.

   Stop the Docker Services: After the job is completed, stop the Docker services:
   ```bash
   docker-compose down
   ```

## Project structure
   ```bash
   spark-job-analysis/
   │
   ├── data/                      # Directory for output CSV files
   ├── spark_app/                 # Main application directory
   │   ├── job_analysis.py        # Python script for the Spark job
   │   ├── ldjson_to_csv.py       # Script to convert LDJSON to CSV
   │   └── helper_functions.py      # Data transformation functions
   ├── .env                        # Environment variables for the project
   ├── .env.spark                  # Environment variables for the Spark environment
   ├── Dockerfile                  # Dockerfile for setting up the Spark environment
   ├── docker-compose.yml          # Configuration file for Docker Compose
   ├── README.md                   # Project documentation and instructions
   ├── requirements.txt            # Python package dependencies
   └── .gitignore                  # Files and directories to ignore in Git
   ```

## UDF performance comment

The UDF (User Defined Function) for cleaning job descriptions using BeautifulSoup has a few performance considerations:

- HTML Parsing Overhead:
The function uses BeautifulSoup, which can introduce significant overhead, especially with larger HTML documents. Parsing with the default html.parser is slower compared to alternatives like lxml. If the job descriptions are extensive, this could lead to longer execution times.

- Tag Removal Efficiency:
The current approach to removing non-visible tags involves iterating over a list of tags and calling decompose() for each element found. This method can be inefficient, especially when dealing with large HTML content, as each call to decompose() involves additional processing overhead.

- Regular Expressions Usage:
The use of regular expressions for whitespace normalization and removing non-printable characters can be CPU-intensive, especially if they are executed multiple times on the same text. Regular expressions are powerful but can become a bottleneck if not optimized.

- Sequential Processing:
If multiple job descriptions are processed sequentially, the function does not leverage the potential for parallel processing, which could significantly speed up execution for large datasets.
