from pyspark.sql import SparkSession

HIVE_METASTORE = "thrift://hive-metastore:9083"
MYSQL_URL = "jdbc:mysql://mysql-db:3306/sourcedb"
MYSQL_USER = "root"
MYSQL_PASS = "root"
MYSQL_TABLE = "orders"

HDFS_PATH = "hdfs://namenode:9000/datalake/orders"  # đường dẫn HDFS

spark = (
    SparkSession.builder
    .appName("MySQL_to_HDFS_to_Hive")
    .config("hive.metastore.uris", HIVE_METASTORE)
    .enableHiveSupport()
    .getOrCreate()
)

print(">>> Đọc dữ liệu từ MySQL ...")
df = spark.read.format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", MYSQL_TABLE) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASS) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

df.printSchema()
df.show(5, truncate=False)

print(">>> Ghi vào HDFS (Data Lake) dưới dạng Parquet ...")
df.write.mode("overwrite").parquet(HDFS_PATH)

print(">>> Tạo database Hive nếu chưa có ...")
spark.sql("CREATE DATABASE IF NOT EXISTS dw")

print(">>> Tạo bảng Hive (external) trỏ tới HDFS ...")
spark.sql("""
  CREATE EXTERNAL TABLE IF NOT EXISTS dw.orders (
    id INT,
    order_date DATE,
    customer_name STRING,
    amount DECIMAL(12,2)
  )
  STORED AS PARQUET
  LOCATION 'hdfs://namenode:9000/datalake/orders'
""")

print(">>> Kiểm tra đọc từ Hive ...")
spark.sql("SELECT * FROM dw.orders LIMIT 10").show()

spark.stop()
