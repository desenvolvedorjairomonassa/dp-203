Databricks 

DBC Source (notebookc databricks)
 ** how to import, go to workspace --> user-->import

 *  https://github.com/MicrosoftLearning/DP-203-Data-Engineer/raw/master/Allfiles/microsoft-learning-paths-databricks-notebooks/data-engineering/DBC/04-Working-With-Dataframes.dbc

 * https://github.com/MicrosoftLearning/DP-203-Data-Engineer/raw/master/Allfiles/microsoft-learning-paths-databricks-notebooks/data-engineering/DBC/07-Dataframe-Advanced-Methods.dbc

Problem 1: count articles
Solution  ->  pyspark: 
 df=spark.read.parquet(path).select('article').distinct()
 totalArticles = df.count() # Identify the total number of records remaining.
  print("Distinct Articles: {0:,}".format(totalArticles))
  
Problem 2 : deduplication customer
Solution -> pyspark : 
from pyspark.sql.functions import col, regexp_replace, upper
from pyspark.sql.types import *
df = (spark.read.format("csv").option("header", "true").option("delimiter", ":").load(sourceFile))
df.printSchema()
df= df.withColumn("ssn",regexp_replace("ssn","-",""))\
  .withColumn("firstName", upper(col("firstName")))\
  .withColumn("middleName", upper(col("middleName")))\
  .withColumn("lastName", upper(col("lastName")))\
  .select(col("firstName"),col("middleName"), \
  col("lastName"),col("gender"), col("birthDate"), col("salary"), col("ssn")).distinct()
df.show(3)
print(df.count())
dbutils.fs.rm(destFile, True)
df.write.parquet(destFile)
