#importing dependencies
import requests

def extract_data(spark):


    url = "https://dummyjson.com/products"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Check for HTTP errors

        json_data = response.json()
        products = json_data.get("products", [])
        
        if not products:
            raise ValueError("No data found in API response")
         
        clean_products=[]

        for item in products:
            new_item=item.copy()
                                    
            new_item["price"]=float(item.get("price",0))
            new_item["discountPercentage"]=float(item.get("discountPercentage",0))
            new_item["rating"]=float(item.get("rating",0))                        
            
            clean_products.append(new_item)
        
                                                      
        # Create DataFrame
        df = spark.createDataFrame(clean_products)

        # WRITE RAW DATA
        (
        df.write 
          .mode("overwrite")    
          .json("data/raw/products_json")
        ) 

        print(" Raw data written successfully")

        return df

    except Exception as e:
        print(f" Extraction failed: {e}")
        raise          
