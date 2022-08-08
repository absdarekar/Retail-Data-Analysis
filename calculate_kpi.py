from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType, LongType, \
    TimestampType
from pyspark.sql.functions import from_json, col, explode

spark = SparkSession\
    .builder\
    .appName("calculate_kpi")\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

items_schema = StructType\
    (
        [
            StructField(name="SKU", dataType=StringType(), nullable=True),
            StructField(name="title", dataType=StringType(), nullable=True),
            StructField(name="unit_price", dataType=FloatType(), nullable=True),
            StructField(name="quantity", dataType=IntegerType(), nullable=True)
        ]
    )

invoice_schema = StructType\
    (
        [
            StructField(name="items", dataType=ArrayType(items_schema), nullable=True),
            StructField(name="type", dataType=StringType(), nullable=True),
            StructField(name="country", dataType=StringType(), nullable=True),
            StructField(name="invoice_no", dataType=LongType(), nullable=True),
            StructField(name="timestamp", dataType=TimestampType(), nullable=True)
        ]
    )

invoices = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "18.211.252.152:9092")\
    .option("subscribe", "real-time-project")\
    .load()\
    .select(from_json(col("value").cast("string"), invoice_schema).alias("invoice"))\
    .select(col("invoice.*"))\
    .select("invoice_no", "country", "timestamp", "type", explode("items").alias("items"))\
    .select("invoice_no", "country", "timestamp", "type", "items.SKU", "items.title", "items.unit_price",
            "items.quantity")

write_invoices = invoices\
    .writeStream\
    .outputMode("append")\
    .format("console")\
    .option("truncate", "false")\
    .start()

write_invoices.awaitTermination()
