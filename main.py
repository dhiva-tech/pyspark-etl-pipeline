

from pyspark.sql import SparkSession
from extract import extract_data
from transform import transform_data
from load import load_data


def main():

    print("ETL Pipeline Started")

    spark = SparkSession.builder \
        .appName("ETL Project") \
        .getOrCreate()

    try:
        df_raw = extract_data(spark)
        df_transformed = transform_data(df_raw)
        load_data(df_transformed)

        print("ETL Pipeline completed successfully")

    except Exception as e:
        print(f"Pipeline failed: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    