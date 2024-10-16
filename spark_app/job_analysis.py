from pyspark.sql import SparkSession

from ldjson_to_csv import download_and_convert
from spark_app.helper_functions import calculate_jobs_per_city, calculate_avg_salary, top_companies, clean_job_descriptions


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
    df_cleaned, df_cleaned_only = clean_job_descriptions(df)

    # Export results to CSV
    df_jobs_city.coalesce(1).write.csv("/opt/bitnami/spark/data/daily_city_jobs", header=True, mode='overwrite')
    df_avg_salary.coalesce(1).write.csv("/opt/bitnami/spark/data/avg_salary", header=True, mode='overwrite')
    df_top_companies.coalesce(1).write.csv("/opt/bitnami/spark/data/top_companies", header=True, mode='overwrite')
    df_cleaned.coalesce(1).write.csv("/opt/bitnami/spark/data/cleaned_descriptions", header=True, mode='overwrite')
    df_cleaned_only.coalesce(1).write.csv("/opt/bitnami/spark/data/cleaned_descriptions_only", header=True, mode='overwrite')


if __name__ == "__main__":
    download_and_convert()
    main()
