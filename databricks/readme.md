Databricks 

DBC Source (notebookc databricks)
 ** how to import, go to workspace --> user-->import

 *  https://github.com/MicrosoftLearning/DP-203-Data-Engineer/raw/master/Allfiles/microsoft-learning-paths-databricks-notebooks/data-engineering/DBC/04-Working-With-Dataframes.dbc

 * https://github.com/MicrosoftLearning/DP-203-Data-Engineer/raw/master/Allfiles/microsoft-learning-paths-databricks-notebooks/data-engineering/DBC/07-Dataframe-Advanced-Methods.dbc

#Problem 1: count articles <br>
Solution  ->  pyspark: <br>
 df=spark.read.parquet(path).select('article').distinct() <br>
 totalArticles = df.count() # Identify the total number of records remaining. <br>
  print("Distinct Articles: {0:,}".format(totalArticles)) <br>
  
#Problem 2 : deduplication customer <br>
Solution -> pyspark : <br>
from pyspark.sql.functions import col, regexp_replace, upper <br>
from pyspark.sql.types import *    <br>
df = (spark.read.format("csv").option("header", "true").option("delimiter", ":").load(sourceFile)) <br>
df.printSchema() <br>
df= df.withColumn("ssn",regexp_replace("ssn","-",""))\  <br>
  .withColumn("firstName", upper(col("firstName")))\    <br>
  .withColumn("middleName", upper(col("middleName")))\  <br>
  .withColumn("lastName", upper(col("lastName")))\      <br>
  .select(col("firstName"),col("middleName"), \          <br>
  col("lastName"),col("gender"), col("birthDate"), col("salary"), col("ssn")).distinct() <br>
df.show(3)     <br>
print(df.count())  <br>
dbutils.fs.rm(destFile, True) <br>
df.write.parquet(destFile)    <br>
