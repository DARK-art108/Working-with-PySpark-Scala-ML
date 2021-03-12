// Databricks notebook source
// MAGIC %scala
// MAGIC 
// MAGIC 1+1

// COMMAND ----------

val mushroom = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ",")
  .load("/FileStore/tables/mushroo___mushroo.csv")

mushroom.show()

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC mushroom.printSchema()

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC mushroom.describe().show()

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC mushroom.show(5)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC mushroom.createOrReplaceTempView("MushroomData")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from MushroomData;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(class), 
// MAGIC CASE 
// MAGIC 	WHEN class = "e" THEN "Edible"
// MAGIC 	ELSE "Poisonous"
// MAGIC END AS CLASSES,
// MAGIC bruises from MushroomData group by CLASSES, bruises; 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(capcolor), 
// MAGIC CASE 
// MAGIC 	WHEN capcolor = "n" THEN "Brown"
// MAGIC 	WHEN capcolor = "b" THEN "Buff"
// MAGIC 	WHEN capcolor = "c" THEN "Cinnamon"
// MAGIC 	WHEN capcolor = "g" THEN "Gray"
// MAGIC 	WHEN capcolor = "r" THEN "Green"
// MAGIC 	WHEN capcolor = "p" THEN "Pink"
// MAGIC 	WHEN capcolor = "u" THEN "Purple"
// MAGIC 	WHEN capcolor = "e" THEN "Red"
// MAGIC 	WHEN capcolor = "w" THEN "White"
// MAGIC 	ELSE "Yellow"
// MAGIC END AS ColorOfCap 
// MAGIC from MushroomData group by capcolor order by count(capcolor) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(capcolor),
// MAGIC CASE 
// MAGIC 	WHEN class = "e" THEN "Edible"
// MAGIC 	ELSE "Poisonous"
// MAGIC END AS CLASSES,
// MAGIC CASE 
// MAGIC 	WHEN capcolor = "n" THEN "Brown"
// MAGIC 	WHEN capcolor = "b" THEN "Buff"
// MAGIC 	WHEN capcolor = "c" THEN "Cinnamon"
// MAGIC 	WHEN capcolor = "g" THEN "Gray"
// MAGIC 	WHEN capcolor = "r" THEN "Green"
// MAGIC 	WHEN capcolor = "p" THEN "Pink"
// MAGIC 	WHEN capcolor = "u" THEN "Purple"
// MAGIC 	WHEN capcolor = "e" THEN "Red"
// MAGIC 	WHEN capcolor = "w" THEN "White"
// MAGIC 	ELSE "Yellow"
// MAGIC END AS ColorOfCap 
// MAGIC from MushroomData group by capcolor,class order by count(capcolor) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(odor), 
// MAGIC CASE 
// MAGIC 	WHEN odor = "a" THEN "almond"
// MAGIC 	WHEN odor = "l" THEN "anise"
// MAGIC 	WHEN odor = "c" THEN "creosote"
// MAGIC 	WHEN odor = "y" THEN "fishy"
// MAGIC 	WHEN odor = "f" THEN "foul"
// MAGIC 	WHEN odor = "m" THEN "musty"
// MAGIC 	WHEN odor = "n" THEN "none"
// MAGIC 	WHEN odor = "p" THEN "pungent"
// MAGIC 	ELSE "spicy"
// MAGIC END AS odor 
// MAGIC from MushroomData group by odor order by count(odor) desc;  

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(odor), 
// MAGIC CASE 
// MAGIC 	WHEN class = "e" THEN "Edible"
// MAGIC 	ELSE "Poisonous"
// MAGIC END AS CLASSES,
// MAGIC CASE 
// MAGIC 	WHEN odor = "a" THEN "almond"
// MAGIC 	WHEN odor = "l" THEN "anise"
// MAGIC 	WHEN odor = "c" THEN "creosote"
// MAGIC 	WHEN odor = "y" THEN "fishy"
// MAGIC 	WHEN odor = "f" THEN "foul"
// MAGIC 	WHEN odor = "m" THEN "musty"
// MAGIC 	WHEN odor = "n" THEN "none"
// MAGIC 	WHEN odor = "p" THEN "pungent"
// MAGIC 	ELSE "spicy"
// MAGIC END AS odor 
// MAGIC from MushroomData group by odor, class order by count(odor) desc; 

// COMMAND ----------


