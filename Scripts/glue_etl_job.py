from awsglue.context import GlueContext
from pyspark.context import SparkContext
import pyspark.sql.functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

raw_path = "s3://my-retail-raw-2025/retail_transactions/"
processed_path = "s3://my-retail-processed-2025/retail_transactions/"

# Load CSV
df = spark.read.option("header", True).csv(raw_path)

# Clean & transform
df_clean = (df
    .withColumn("Sales", F.col("Sales").cast("double"))
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("Order_Date", F.to_date(F.col("Order Date"), "MM/dd/yyyy"))
    .dropna(subset=["Sales", "Quantity", "Order_Date"])
    .withColumn("year", F.year("Order_Date"))
    .withColumn("month", F.month("Order_Date"))
)

# Save cleaned data as Parquet partitioned by year/month
df_clean.write.mode("overwrite").partitionBy("year","month").parquet(processed_path)