from pyspark.sql.functions import udf
from bs4 import BeautifulSoup
import re
from pyspark.sql.types import StringType


# UDF to clean HTML tags from job descriptions
def clean_html(description: str) -> str:
    if description:
        soup = BeautifulSoup(description, "html.parser")
        return re.sub(r'\s+', ' ', soup.get_text()).strip()
    return description

clean_html_udf = udf(clean_html, StringType())
