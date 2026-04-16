def load_data(df):

    print(" Starting loading process...")

    try:
        df.write \
            .mode("overwrite") \
            .parquet("data/processed/products")

        print(" Data saved as Parquet successfully")

    except Exception as e:
        print(f" Loading failed: {e}")
        raise

    print(" Loading completed successfully")

