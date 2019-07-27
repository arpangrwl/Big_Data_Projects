// Dates and TimeStamps

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

// Show Schema
df.printSchema()

// Lot's of options here
//
df.select(month(df("Date"))).show()

df.select(year(df("Date"))).show()

df.select(dayofmonth(df("Date"))).show()

// Practical Example
val df2 = df.withColumn("Year",year(df("Date")))

// Mean per Year, notice large 2008 drop!
val dfavgs = df2.groupBy("Year").mean()
dfavgs.select($"Year",$"avg(Close)").show()
