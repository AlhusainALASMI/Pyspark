from itertools import count

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, max, day, month



spark = SparkSession.builder.appName("Sales Data Analysis").getOrCreate()

# Load the sales data CSV file
sales_df = spark.read.csv('data/sales_data.csv', header=True, inferSchema=True)
sales_df.show()

#Exercise 1: Total Sales by Region
sales_df = sales_df.withColumn('TotalSale', col('Quantity') * col('Price'))

sales_df.show()
total_sales_by_region = sales_df.groupBy('Region').agg(sum(col('TotalSale')).alias('TotalSale')).orderBy(col("TotalSale").desc())

total_sales_by_region.show()

#Exercise 2: Find the Average Quantity Sold by Category
average_category_quantity = sales_df.groupby('Category').agg(avg(col('Quantity')).alias('Average quanity'))

average_category_quantity.show()

#Exercise 3: Filter Orders with High Total Sales
sales_df.select("OrderID", "Product", "TotalSale", "OrderDate").filter("TotalSale > 500").show()

#Exercise 4: Calculate the Number of Orders in Each Region
Num_order_region = sales_df.groupby("Region").agg(count("OrderID").alias("Number of Orders"))
Num_order_region.show()

#Exercise 5: Calculate Total Sales by Product
Total_sales_product = sales_df.groupby("Product").agg(sum("TotalSale").alias("Total sales by product"))
Total_sales_product.show()

#Exercise 6: Find the Most Expensive Product
most_expensive_product = sales_df.orderBy(col('Price').desc()).first()
print(f"Most Expensive Product: {most_expensive_product['Product']} | Price: {most_expensive_product['Price']}")

#Exercise 7: Calculate the Average Price of Products by Category
average_category_price = sales_df.groupby('Category').agg(avg(col('Price')).alias('Average Price'))
average_category_price.show()

#Exercise 8: Find the Total Sales of Electronics in the East Region
Total_sales_east = sales_df.filter("Region == 'East'")
Total_sales_east.show()

#Exercise 9: Find Products Sold in the Highest Quantity
most_expensive_product = sales_df.orderBy(col('Quantity').desc()).limit(3)
most_expensive_product.show()

#Exercise 10: Calculate the Sales for Each Day
total_sales_per_day = sales_df.groupBy('OrderDate').agg(sum('TotalSale').alias('TotalSales'))

total_sales_per_day.orderBy(col('OrderDate').asc()).show()

#Exercise 11: Filter Orders for Products in the Apparel Category Sold in January
sales_df.select("OrderID", "Product", "TotalSale", "OrderDate") \
    .filter((month("OrderDate") == 1) & (col("Category") == "Apparel")) \
    .show()
#Exercise 12: Find the Top 5 Highest Sales Per Order
highest_top_5 = sales_df.orderBy(col('TotalSale').desc()).limit(5)
highest_top_5.show()
