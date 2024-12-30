catalog = ""
table_name = ""
checkpoint = ""
start_date = ""
end_date = ""
load_path = ""

df_read = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxFilesPerTrigger", "5000")
    .option("modifiedAfter", start_date)
    .option("modifiedBefore", end_date)
    .option("nullValue", "NULL")
    .option("quote", '"')
    .option("multiline", True)
    .option("escape", '"')
    .load(load_path)
)

(
    df_read.writeStream.format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint)
    .option("delta.columnMapping.mode", "name")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.bronze.{table_name}")
    .awaitTermination()
)
