from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

def create_spark_session():
    return (
        SparkSession.builder.appName("KafkaStructuredStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

def train_ml_model(static_data):
    # Convert categorical columns to numerical format
    categorical_cols = ["Bathrooms", "Neighborhood"]
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded") for col in categorical_cols]

    # Create a feature vector using VectorAssembler
    assembler = VectorAssembler(inputCols=["SquareFeet", "Bedrooms", "Bathrooms_encoded", "Neighborhood_encoded", "YearBuilt", "Price"], outputCol="Cfeatures")

    # Add a "label" column to the static data
    static_data = static_data.withColumn("label", col("SquareFeet").cast(IntegerType()))

    # Create a logistic regression model
    lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")

    # Create a pipeline for the transformations and model
    pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])
    model = pipeline.fit(static_data)

    return model

def main():
    # Create a Spark session
    spark = create_spark_session()
    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "proje"

    # Define the schema for the data
    schema = StructType().add("SquareFeet", IntegerType()).add("Bedrooms", IntegerType()) \
        .add("Bathrooms", IntegerType()).add("Neighborhood", StringType()) \
        .add("YearBuilt", IntegerType()).add("Price", DoubleType())

    # Read static data from a CSV file
    static_data_path = "C:/housing.csv"
    static_data = spark.read.csv(static_data_path, header=True, schema=schema)

    # Read streaming data from Kafka
    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    json_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) as value")

    # Parse the JSON data and select fields
    parsed_stream_df = json_stream_df.select(
        from_json("value", schema).alias("data")
    ).select("data.*")

    # Add a "label" column to the streaming data
    labeled_stream_df = parsed_stream_df.withColumn("label", col("SquareFeet").cast(IntegerType()))

    # Filter the streaming data based on a condition
    processed_stream_df = labeled_stream_df.filter("SquareFeet > 2000")

    # Train or load the machine learning model
    ml_model = train_ml_model(static_data)

    # Use the machine learning model
    ml_result = ml_model.transform(processed_stream_df)

    # Write the results to the console in append mode
    query = (
        ml_result.writeStream.outputMode("append")
        .format("console")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()

# Check if the script is being run as the main program
if __name__ == "__main__":
    main()
