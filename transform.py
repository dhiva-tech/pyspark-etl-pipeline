# importing dependencies
from pyspark.sql.functions import col, when, round

def transform_data(df):

    print(" Starting transformation...")
    
    #---------------------------
    #1.cast
    #---------------------------

    df = df.withColumn("price", col("price").cast("double"))
    df = df.withColumn("discountPercentage", col("discountPercentage").cast("double"))
    df = df.withColumn("rating", col("rating").cast("double"))
    df = df.withColumn("stock", col("stock").cast("int"))
    
    df.printSchema()

    # ---------------------------
    # 2. Drop unnecessary columns
    # ---------------------------
    df = df.drop("description", "thumbnail")

    #---------------------------
    #3. Handling null values
    df = df.filter(col('brand').isNotNull() & col('category').isNotNull())
    #---------------------------

    # ---------------------------
    # 4. Rename columns
    # ---------------------------
    df = df.withColumnRenamed("discountPercentage", "discount_pct")

    # ---------------------------
    # 5. Calculate final price
    # ---------------------------
    df = df.withColumn(
        "final_price",
        round(
            col("price") - (col("price") * col("discount_pct") / 100),
            2
        )
    )

    # ---------------------------
    # 6. Price category
    # ---------------------------
    df = df.withColumn(
        "price_category",
        when(col("final_price") < 50, "Low")
        .when(col("final_price") < 200, "Medium")
        .otherwise("High")
    )

    # ---------------------------
    # 7. Rating category
    # ---------------------------
    df = df.withColumn(
        "rating_category",
        when(col("rating") >= 4, "Excellent")
        .when(col("rating") >= 3, "Good")
        .otherwise("Average")
    )

    # ---------------------------
    # 8. Discount level
    # ---------------------------
    df = df.withColumn(
        "discount_level",
        when(col("discount_pct") >= 15, "High Discount")
        .when(col("discount_pct") >= 5, "Medium Discount")
        .otherwise("Low Discount")
    )

    # ---------------------------
    # 9. Stock status
    # ---------------------------
    df = df.withColumn(
        "stock_status",
        when(col("stock") > 50, "Available")
        .otherwise("Low Stock")
    )

    # ---------------------------
    # 10. Estimated profit (example logic)
    # ---------------------------
    df = df.withColumn(
        "estimated_profit",
        round(col("final_price") * 0.3, 2)
    )

    #---------------------------
    #11.High demand category
    # --------------------------
    df = df.withColumn(
        "high_demand",
        when((col("rating") >= 4) & (col("stock") < 20), "Yes").otherwise("No")
    )


    print(" Transformation completed successfully")

    return df
