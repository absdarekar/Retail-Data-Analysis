from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from ast import literal_eval
from pyspark.sql.functions import from_json, col, udf, when

spark = SparkSession \
    .builder \
    .appName("calculate_kpis") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

invoice_schema = StructType(
    [
        StructField(name="items", dataType=StringType(), nullable=True),
        StructField(name="type", dataType=StringType(), nullable=True),
        StructField(name="country", dataType=StringType(), nullable=True),
        StructField(name="invoice_no", dataType=LongType(), nullable=True),
        StructField(name="timestamp", dataType=TimestampType(), nullable=True)
    ]
)

invoices = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe", "real-time-project") \
    .load() \
    .select(from_json(col("value").cast("string"), invoice_schema).alias("invoice")) \
    .select(col("invoice.*")) \
    .select("invoice_no", "country", "timestamp", "type", "items")


def calculate_total_cost(orders):
    orders = literal_eval(orders)
    total_cost = 0
    for order in orders:
        total_cost += order["unit_price"] * order["quantity"]
    return total_cost


def sum_items(orders):
    orders = literal_eval(orders)
    total_orders = 0
    for order in orders:
        total_orders += order["quantity"]
    return total_orders


def is_order(order_type):
    if order_type == "ORDER":
        return 1
    return 0


def is_return(order_type):
    if order_type == "RETURN":
        return 1
    return 0


calculate_total_cost_udf = udf(lambda orders: calculate_total_cost(orders))
sum_items_udf = udf(lambda orders: sum_items(orders))
is_order_udf = udf(lambda order_type: is_order(order_type))
is_return_udf = udf(lambda order_type: is_return(order_type))

invoices_enriched = invoices \
    .withColumn("is_order", is_order_udf("type")) \
    .withColumn("is_return", is_return_udf("type")) \
    .withColumn("total_cost",
                when(col("is_return") == 1, -1 * calculate_total_cost_udf("items"))
                .otherwise(calculate_total_cost_udf("items"))
                ) \
    .withColumn("total_items", sum_items_udf("items")) \
    .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return")

invoices_enriched \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()
