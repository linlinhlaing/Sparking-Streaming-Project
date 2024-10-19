import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Spark session
spark = SparkSession.builder \
    .appName("WeatherSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.1") \
    .getOrCreate()

# Define the schema for the weather data
schema = StructType([
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("city", StringType(), True),
    StructField("description", StringType(), True),
    StructField("timestamp", StringType(), True)  # Ensure this is a unique identifier
])

# Read data from Kafka
weather_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather") \
    .load()

# Convert the Kafka value (which is in bytes) to a JSON object
weather_data = weather_stream.selectExpr("CAST(value AS STRING)")

# Parse the JSON string into columns
parsed_weather_data = weather_data.select(from_json(col("value"), schema).alias("data"))

# Select the relevant fields
final_weather_data = parsed_weather_data.select("data.*")

# Function to store data in HBase
def store_in_hbase(df):
    # Each partition needs its own connection to HBase
    def put_partition(partition):
        # Create a connection to HBase inside each partition
        connection = happybase.Connection('localhost')
        table = connection.table('weather_data')
        
        # Iterate over the partition rows
        for row in partition:
            try:
                # Prepare data to store in HBase, handling None values
                data = {
                    b'info:temperature': str(row['temperature']).encode('utf-8') if row['temperature'] is not None else b'',
                    b'info:humidity': str(row['humidity']).encode('utf-8') if row['humidity'] is not None else b'',
                    b'info:city': row['city'].encode('utf-8') if row['city'] is not None else b'',
                    b'info:description': row['description'].encode('utf-8') if row['description'] is not None else b''
                }
                
                # Store the data in HBase using the 'timestamp' as the row key
                if row['timestamp'] is not None:
                    row_key = row['timestamp'].encode('utf-8')
                    table.put(row_key, data)
                    logger.info(f"Inserted row with key {row_key} into HBase.")
                else:
                    logger.warning("Row timestamp is None. Not inserting into HBase.")
            except Exception as e:
                logger.error(f"Failed to insert row: {e}")
        
        # Close the connection
        connection.close()

    # Apply the function to each partition
    df.foreachPartition(put_partition)

# Start the streaming query to store the output in HBase
query = final_weather_data.writeStream \
    .foreachBatch(lambda df, epoch_id: store_in_hbase(df)) \
    .outputMode("update") \
    .start()

query.awaitTermination()
