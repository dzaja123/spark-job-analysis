import pandas as pd
import json
import os

from kaggle.api.kaggle_api_extended import KaggleApi


# Note: Kaggle API token must be stored in C:/Users/<username>/.kaggle/kaggle.json
# Initialize Kaggle API
api = KaggleApi()
api.authenticate()


# Function to download the dataset
def download_dataset(dataset: str, output_directory: str) -> None:
    print("Downloading dataset...")

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Download the dataset
    api.dataset_download_files(dataset, path=output_directory, unzip=True)

    print("Download completed: The dataset has been extracted in the directory:", output_directory)


def ldjson_to_csv(input_file: str, output_file: str) -> None:
    # Read the LDJSON file with proper encoding and handle each line as a separate JSON object
    data = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data.append(json.loads(line.strip()))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line: {line[:100]}...")  # Print a portion of the problematic line for debugging
                print(f"Error details: {e}")

    # Convert the list of dictionaries into a pandas DataFrame
    df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file
    df.to_csv(output_file, index=False, encoding='utf-8')

    print(f"Conversion complete! Data saved to {output_file}")


def download_and_convert() -> None:
    # Download the dataset
    dataset = "promptcloud/careerbuilder-job-listing-2020"
    output_directory = "/opt/bitnami/spark/dataset"

    input_file = "/opt/bitnami/spark/dataset/marketing_sample_for_careerbuilder_usa-careerbuilder_job_listing__20200401_20200630__30k_data.ldjson"
    output_file = "/opt/bitnami/spark/dataset/JobList.csv"

    # Download and extract the dataset
    download_dataset(dataset=dataset, output_directory=output_directory)

    # Convert LDJSON to CSV
    ldjson_to_csv(input_file, output_file)


if __name__ == "__main__":
    download_and_convert()