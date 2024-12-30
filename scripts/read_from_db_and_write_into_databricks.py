import pytz
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

catalog = ""
table_name = ""
query = ""
host = ""
port = ""
dbname = ""
user = ""
password = ""
partition_col = ""
start = ""
end = ""

df = (
    spark.read.format("postgresql")
    .option("dbtable", f"({query}) as source_table")
    .option("host", host)
    .option("port", port)
    .option("database", dbname)
    .option("user", user)
    .option("password", password)
    .option("partitionColumn", partition_col)
    .option("lowerBound", f"{start}T00:00:00")
    .option("upperBound", f"{end}T00:00:00")
    .option("numPartitions", "10")
    .load()
)

df = df.withColumn(
    "timestamp_ingestion", lit(datetime.now(pytz.timezone("America/Sao_Paulo")))
)
df = df.withColumn("filename", lit(f"Data Recovery - {datetime.now()}"))
columns = ",".join(df.columns)
target_delta = DeltaTable.forName(spark, f"{catalog}.bronze.{table_name}")
df.createOrReplaceTempView("temp_table")
spark.sql(
    f"INSERT INTO {catalog}.bronze.{table_name} ({columns}) SELECT {columns} FROM temp_table"
)
