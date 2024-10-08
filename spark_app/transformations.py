from pyspark.sql.functions import col, count, avg, desc
from pyspark.sql.functions import to_date
from pyspark.sql import DataFrame


def calculate_jobs_per_city(df: DataFrame) -> DataFrame:
    df = df.withColumn("posted_date", to_date(col("post_date"), 'yyyy-MM-dd'))
    return df.groupBy("posted_date", "city") \
             .agg(count("*").alias("job_count")) \
             .orderBy("posted_date", "city")


# TODO: Add better average salary calculations
# This doesn't work yet
def calculate_avg_salary(df: DataFrame) -> DataFrame:
    return df.groupBy("job_title", "state") \
             .agg(avg(col("salary_offered")).alias("avg_salary"))


def top_companies(df: DataFrame) -> DataFrame:
    return df.groupBy("company_name") \
             .agg(count("*").alias("job_openings")) \
             .orderBy(desc("job_openings")) \
             .limit(10)
