from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from ldjson_to_csv import download_and_convert
from transformations import calculate_jobs_per_city, calculate_avg_salary, top_companies

from bs4 import BeautifulSoup
import re


# UDF to clean HTML tags from job descriptions
def clean_html(description: str) -> str:
    if description:
        soup = BeautifulSoup(description, "html.parser")
        return re.sub(r'\s+', ' ', soup.get_text()).strip()
    return description


def main() -> None:
    # Create Spark session
    spark = SparkSession.builder.appName("Job Analysis").getOrCreate()

    # Load dataset
    data_path = "/opt/bitnami/spark/dataset/JobList.csv"
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Print summary statistics
    df.describe()

    # Display the data
    df.show(n=5)

    # Print schema for debugging
    df.printSchema()

    # Perform transformations
    df_jobs_city = calculate_jobs_per_city(df)
    df_avg_salary = calculate_avg_salary(df)
    df_top_companies = top_companies(df)

    # Clean job descriptions
    # TODO: Add better cleaning logic
    clean_html_udf = udf(clean_html, StringType())
    df_cleaned = df.withColumn("cleaned_description", clean_html_udf(df['job_description']))

    # Export results to CSV
    df_jobs_city.write.csv("/opt/bitnami/spark/data/daily_city_jobs", header=True, mode='overwrite')
    df_avg_salary.write.csv("/opt/bitnami/spark/data/avg_salary", header=True, mode='overwrite')
    df_top_companies.write.csv("/opt/bitnami/spark/data/top_companies", header=True, mode='overwrite')
    df_cleaned.write.csv("/opt/bitnami/spark/data/cleaned_descriptions", header=True, mode='overwrite')


if __name__ == "__main__":
    download_and_convert()
    main()
