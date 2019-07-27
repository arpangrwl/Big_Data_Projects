// Problem 1.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val purchase = spark.read.format("csv").option("sep",",").option("inferSchema","true").option("header","true").load("/Users/hao/Desktop/wpi/DS503_S19/project3/Purchase1.csv")

val customers = spark.read.format("csv").option("sep",",").option("inferSchema","true").option("header","true").load("/Users/hao/Desktop/wpi/DS503_S19/project3/Customers1.csv")

// (1)
purchase.filter($"TransTotal">500).show()

// (2)
val purchase1 = purchase.filter($"TransTotal">500)
val purchase2 = purchase1.groupBy("TransNumItems").agg(avg(purchase1("TransTotal")),min(purchase1("TransTotal")),max(purchase1("TransTotal")))

// (3)
purchase2.show()

// (4)
val customers1 = customers.select($"ID",$"Age").filter("10 < Age AND Age < 20")
val purchase3 = purchase1.groupBy("CustID").agg(sum(purchase1("TransNumItems")),sum(purchase1("TransTotal")))
val groupdata = purchase3.join(customers1,$"ID" ===$"CustID","inner").select(customers1("ID"),customers1("Age"),purchase3("sum(TransNumItems)"),purchase3("sum(TransTotal)"))

// (5)
val groupdata1 = groupdata

val name_1 = Seq("C1_ID","Age1","TotalItemCount1","TotalAmount1")
val name_2 = Seq("C2_ID","Age2","TotalItemCount2","TotalAmount2")
val group1 = groupdata.toDF(name_1:_*)
val group2 = groupdata1.toDF(name_2:_*)

val group3 = group1.join(group2,$"Age1"<$"Age2" && $"TotalItemCount1" < $"TotalItemCount2"&& $"TotalAmount1">$"TotalAmount2")

// (6)
group3.select("C1_ID","C2_ID","Age1","Age2","TotalAmount1","TotalAmount2","TotalItemCount1","TotalItemCount2").show()
