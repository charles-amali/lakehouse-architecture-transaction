import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from delta import *
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder \
    .appName("DeltaLakeETLJob") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


orders_schema = StructType([
    StructField("order_num", IntegerType(), nullable=True),
    StructField("order_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("order_timestamp", TimestampType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("date", DateType(), nullable=False)
])

order_items_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("order_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("days_since_prior_order", IntegerType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("add_to_cart_order", IntegerType(), nullable=True),
    StructField("reordered", IntegerType(), nullable=True),
    StructField("order_timestamp", TimestampType(), nullable=False),
    StructField("date", DateType(), nullable=False)
])

products_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=True),
    StructField("department", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=False)
])


try:
    orders_df = spark.read.option("header", "true").schema(orders_schema).csv("s3://delta-lake-bkt01/raw/orders/")
    order_items_df = spark.read.option("header", "true").schema(order_items_schema).csv("s3://delta-lake-bkt01/raw/order_items/")
    products_df = spark.read.option("header", "true").schema(products_schema).csv("s3://delta-lake-bkt01/raw/products/")
    logger.info("Raw data successfully read from S3")
except Exception as e:
    logger.error(f"Error reading raw data: {e}")
    job.commit()
    sys.exit(1)


try:
    valid_orders_df = orders_df.dropDuplicates(["order_id"]).filter("order_id IS NOT NULL AND order_timestamp IS NOT NULL")
    invalid_orders_df = orders_df.subtract(valid_orders_df)
    invalid_orders_df.write.mode("overwrite").csv("s3://delta-lake-bkt01/rejected/orders/")

    valid_order_items_df = order_items_df.dropDuplicates(["order_id", "product_id"]).filter("order_id IS NOT NULL AND product_id IS NOT NULL")
    invalid_order_items_df = order_items_df.subtract(valid_order_items_df)
    invalid_order_items_df.write.mode("overwrite").csv("s3://delta-lake-bkt01/rejected/order_items/")

    valid_products_df = products_df.dropDuplicates(["product_id"]).filter("product_id IS NOT NULL")
    invalid_products_df = products_df.subtract(valid_products_df)
    invalid_products_df.write.mode("overwrite").csv("s3://delta-lake-bkt01/rejected/products/")

    logger.info("Validation and rejection of invalid records completed")
except Exception as e:
    logger.error(f"Data validation error: {e}")
    job.commit()
    sys.exit(1)


try:
    valid_order_ids = valid_orders_df.select("order_id").distinct()
    valid_product_ids = valid_products_df.select("product_id").distinct()

    valid_order_items_df = valid_order_items_df \
        .join(valid_order_ids, on="order_id", how="inner") \
        .join(valid_product_ids, on="product_id", how="inner")

    logger.info("Referential integrity checks passed")
except Exception as e:
    logger.error(f"Referential integrity error: {e}")
    job.commit()
    sys.exit(1)


# Add Timestamps and Partitions
try:
    valid_orders_df = valid_orders_df.withColumn("order_timestamp", to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm:ss")) \
                                     .withColumn("order_date", col("order_timestamp").cast("date"))

    valid_order_items_df = valid_order_items_df.withColumn("order_timestamp", to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm:ss")) \
                                               .withColumn("order_date", col("order_timestamp").cast("date"))

    logger.info("Timestamp conversion and partition column added")
except Exception as e:
    logger.error(f"Timestamp conversion error: {e}")
    job.commit()
    sys.exit(1)


# Step 6: Merge to Delta Tables
def merge_delta(df, path, key_cols, partition_col):
    try:
        logger.info(f"Writing to Delta table at {path}")
        if DeltaTable.isDeltaTable(spark, path):
            delta_table = DeltaTable.forPath(spark, path)
            merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_cond
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info(f"MERGE completed for {path}")
        else:
            df.write.format("delta").partitionBy(partition_col).save(path)
            logger.info(f"Delta table initialized at {path}")
    except Exception as e:
        logger.error(f"Error writing to Delta table {path}: {e}")
        raise

try:
    merge_delta(valid_orders_df, "s3://delta-lake-bkt01/processed/orders", ["order_id"], "order_date")
    merge_delta(valid_order_items_df, "s3://delta-lake-bkt01/processed/order_items", ["order_id", "product_id"], "order_date")
    merge_delta(valid_products_df, "s3://delta-lake-bkt01/processed/products", ["product_id"], "department_id")
except Exception as e:
    logger.error(f"Delta merge failed: {e}")
    job.commit()
    sys.exit(1)

job.commit()
logger.info("Glue job completed successfully")
