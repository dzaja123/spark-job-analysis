from pyspark.sql.functions import col, count, avg, desc
from pyspark.sql.functions import to_date
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from bs4 import BeautifulSoup

import re
import html


def extract_salary(salary_str):
    if not salary_str:
        return None

    # Remove dollar signs, commas, and any extra spaces
    salary_str = salary_str.replace('$', '').replace(',', '').strip().lower()

    # Remove any trailing slashes and keep only the part before slashes
    salary_str = salary_str.split('/')[0].strip()

    # Remove non-numeric characters, except for ranges and periods
    salary_str = re.sub(r'[^\d\.\- ]', '', salary_str)

    # Handle salary ranges
    if ' - ' in salary_str:
        parts = salary_str.split(' - ')
        # Ensure that there are exactly two valid parts before proceeding
        if len(parts) == 2 and parts[0].strip() and parts[1].strip():
            try:
                low = float(parts[0].strip())
                high = float(parts[1].strip().split()[0])  # Take the first part of the second number
                average_salary = (low + high) / 2

                # If the salary string contains hourly indicators or the average is reasonable for an hourly rate, treat it as such.
                if "hour" in salary_str or "hr" in salary_str or average_salary <= 250:
                    average_salary = average_salary * 2080  # Convert to yearly assuming 40 hours/week

                # Return the monthly salary
                return average_salary / 12
            except ValueError:
                return None
        else:
            # Return None if the range format is invalid
            return None

    # Handle single salaries
    try:
        salary_value = float(salary_str)

        # Check if the salary string indicates an hourly wage or if the value suggests it.
        if "hour" in salary_str or "hr" in salary_str or salary_value <= 250:
            salary_value = salary_value * 2080  # Convert to yearly assuming 40 hours/week

        # If it contains monthly indicators like "month" or "monthly", convert to yearly.
        elif "month" in salary_str:
            salary_value = salary_value * 12

        # Return the monthly salary
        return salary_value / 12
    except ValueError:
        return None


def calculate_avg_salary(df: DataFrame) -> DataFrame:
    extract_salary_udf = udf(extract_salary, FloatType())
    
    # Create a new column with the numeric salary
    df = df.withColumn("salary_numeric", extract_salary_udf(col("salary_offered")))
    
    # Group by job_title and state, and calculate the average monthly salary
    return df.groupBy("job_title", "state") \
             .agg(avg("salary_numeric").alias("avg_monthly_salary"))


def top_companies(df: DataFrame) -> DataFrame:
    return df.groupBy("company_name") \
             .agg(count("*").alias("job_openings")) \
             .orderBy(desc("job_openings")) \
             .limit(10)


def calculate_jobs_per_city(df: DataFrame) -> DataFrame:
    df = df.withColumn("posted_date", to_date(col("post_date"), 'yyyy-MM-dd'))
    return df.groupBy("posted_date", "city") \
             .agg(count("*").alias("job_count")) \
             .orderBy("posted_date", "city")


def clean_html(description: str) -> str:
    if not description:
        return ""
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(description, "html.parser")
    
    # Remove script, style, and other non-visible elements
    for element in soup(["script", "style", "head", "meta", "noscript", "iframe", "span"]):
        element.decompose()
    
    # Remove all HTML attributes like dir, style, class, etc.
    for tag in soup.find_all(True):
        tag.attrs = {}
    
    # Get the visible text from the soup object, using a space separator for better readability
    text = soup.get_text(separator=' ')
    
    # Decode HTML entities (like &amp;, &lt;, etc.)
    text = html.unescape(text)
    
    # Replace multiple spaces, tabs, newlines, and other unwanted characters with a single space
    cleaned_text = re.sub(r'\s+', ' ', text)
    
    # Remove any residual HTML tags, just in case
    cleaned_text = re.sub(r'<.*?>', '', cleaned_text)
    
    # Remove encoded characters and keep printable ASCII characters
    cleaned_text = re.sub(r'[^\x20-\x7E]+', ' ', cleaned_text)
    
    # Strip leading and trailing whitespace
    return cleaned_text.strip()