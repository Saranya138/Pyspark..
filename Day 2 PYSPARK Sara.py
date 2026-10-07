# Databricks notebook source
# MAGIC %md
# MAGIC Difference between DataFrame, Dataset, and RDD in Spark
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pysparkseries').getOrCreate()
columns = ["Seqno","Quote","id"]
data = [("1", "Be the change that you wish to see in the world",1),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself.",2),
    ("3", "The purpose of our lives is to be happy.",3),
    ("4", "Be cool.",4)]
df1 = spark.createDataFrame(data,columns)
df1.show()

# COMMAND ----------

df1 = spark.createDataFrame(data,columns)
df1.show(truncate=False)

# COMMAND ----------

df1.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark StructType & StructField Explained with Examples

# COMMAND ----------

#PySpark infers a schema from data, sometimes we may need to define our own column names and data types 
#PySpark provides from pyspark.sql.types import StructType class to define the structure of the DataFrame.

# COMMAND ----------


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.appName('datsession').getOrCreate()

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("id", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("salary", IntegerType(), True) 
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.appName('pysparkseries').getOrCreate()

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]


 
df = spark.createDataFrame(data=data)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark Select Columns From DataFrame

# COMMAND ----------


df.select("firstname","lastname","middlename").show()

df.select(df.firstname,df.lastname).show()

df.select(df["firstname"],df["lastname"]).show()

#By using col() function
from pyspark.sql.functions import col
df.select(col("firstname"),col("lastname")).show()

#Select columns by regular expression ,selects all columns whose names match a regular expression, so you don't have to list them one by one.
df.select(df.colRegex("`^.*name*`")).show()


# COMMAND ----------


# Select All columns
df.select("*").show()


# COMMAND ----------

#3. Select Columns by Index
#firstname	middlename	lastname	id	  gender	salary
#0	         1	           2	     3	    4	     5
#-6	        -5	           -4	     -3      -2	     -1

#Slice syntax: [start:stop]
df.columns[0]      # 'firstname'  (single name)
df.columns[-1]     # 'salary'     (last)
df.columns[2:4]    # ['lastname', 'id']        (positions 2, 3)
df.columns[:3]     # ['firstname', 'middlename', 'lastname']
df.columns[3:]     # ['id', 'gender', 'salary']
df.columns[:-1]    # everything except the last -> firstname ... gender
df.columns[::2]    # every 2nd column -> firstname, lastname, gender

# COMMAND ----------


#Selects first 3 columns and top 4 rows
df.select(df.columns[:-1]).show(4)

#Selects columns 2 to 4  and top 4 rows
df.select(df.columns[2:4]).show(4)


# COMMAND ----------

#4. Select Nested Struct Columns from PySpark

data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False) # shows all columns




# COMMAND ----------


df2.select("name").show(truncate=False)


# COMMAND ----------


df2.select("*").show(truncate=False)

# COMMAND ----------


df2.select("name.firstname","name.lastname").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark Collect() – Retrieve data from DataFrame

# COMMAND ----------

PySpark RDD/DataFrame collect() is an action operation that is used to retrieve all the elements of the dataset (from all nodes) to the driver node. We should use the collect() on smaller dataset usually after filter(), group() e.t.c. Retrieving larger datasets results in OutOfMemory error.


# COMMAND ----------


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pysparkseries').getOrCreate()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)

dataCollect = deptDF.collect()
print(dataCollect)
deptDF.collect()[1]


# COMMAND ----------

deptDF.collect() #returns Array of Row type.
deptDF.collect()[0] #returns the first element in an array (1st row).
#deptDF.collect[0][0] #returns the value of the first row & first column.

# COMMAND ----------

When to avoid Collect()
Usually, collect() is used to retrieve the action output when you have very small result set and calling collect() on an RDD/DataFrame with a bigger result set causes out of memory as it returns the entire dataset (from all workers) to the driver hence we should avoid calling collect() on a larger dataset.

collect () vs select ()
collect is an action
select() is a transformation that returns a new DataFrame and holds the columns that are selected whereas collect() is an action that returns the entire data set in an Array to the driver.

# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark withColumn()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pysparkseries').getOrCreate()
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

df = spark.createDataFrame(data=data, schema = columns)
df.withColumn("salary",col("salary").cast("string")).show()

df.display()


# COMMAND ----------

from pyspark.sql.functions import col

df.withColumn("salary",col("salary").cast("integer")).show()

df.printSchema()

# COMMAND ----------

df.withColumn("salary",col("salary").cast("string")).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------


df.withColumn("salarys",col("salary")*1000).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark withColumnRenamed to Rename Column on DataFrame
# MAGIC

# COMMAND ----------


df.withColumnRenamed("dob","DateOfBirth").show()



# COMMAND ----------

Since DataFrame’s are an immutable collection, you can’t rename or update a column instead when using withColumnRenamed() it creates a new DataFrame with updated column names, 

# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark Where Filter Function

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("FilterExample").getOrCreate()

# Sample data
data = [("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35),
        ("David", 40),
        ("Eve", 45)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
#df.show()

#Filter
filtered_df = df.filter(df.Age > 30)
filtered_df.show()

#Where condition
filtered_df1=df.where(df.Age > 30)
filtered_df1.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark Distinct to Drop Duplicate Rows

# COMMAND ----------

PySpark distinct() function is used to drop/remove the duplicate rows (all columns) from DataFrame and dropDuplicates() is used to drop rows based on selected (one or multiple) columns.

# COMMAND ----------


# Import pySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create SparkSession
spark = SparkSession.builder.appName('sparkseries').getOrCreate()

# Prepare Data
data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]

# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------


distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)
#distinctDF.count() is an action, so this is where Spark actually runs the job. It returns the number of rows as an integer.
#str(...) converts that integer to text, because you can't join a string and an integer with +.
#"Distinct count: " + ... joins the label and the number.


# COMMAND ----------


df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC distinct() and dropDuplicates() both remove duplicate rows. The difference is that distinct() always compares all columns, while dropDuplicates() can compare only the columns you choose.

# COMMAND ----------


#PySpark Distinct of Selected Multiple Columns
dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department & salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark orderBy() and sort()

# COMMAND ----------

You can use either sort() or orderBy() function of PySpark DataFrame to sort DataFrame by ascending or descending order based on single or multiple columns, you can also do sorting using PySpark SQL sorting functions

# COMMAND ----------


simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------


df.sort("department","state").show(truncate=False)
df.sort(col("department"),col("state")).show(truncate=False)


# COMMAND ----------


df.orderBy("department","state").show(truncate=False)
df.orderBy(col("department"),col("state")).show(truncate=False)


# COMMAND ----------


df.sort(df.department.asc(),df.age.asc()).show(truncate=False)
df.sort(col("department").asc(),col("age").asc()).show(truncate=False)
df.orderBy(col("department").asc(),col("age").asc()).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark Groupby

# COMMAND ----------

PySpark groupBy() function is used to collect the identical data into groups on DataFrame and perform count, sum, avg, min, max functions on the grouped data. In this article, I will explain several groupBy() examples using PySpark (Spark with Python).

# COMMAND ----------


simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------


df.groupBy("department").sum("salary").show(truncate=False)



# COMMAND ----------


df.groupBy("department").min("salary")


# COMMAND ----------



from pyspark.sql.functions import sum,avg,max
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC #PySpark Join Types | Join Two DataFrames

# COMMAND ----------

PySpark Join is used to combine two DataFrames and by chaining these you can join multiple DataFrames; it supports all basic join type operations available in traditional SQL like INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN.

# COMMAND ----------


emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
#empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# COMMAND ----------

# PySpark Inner Join DataFrame

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)


# COMMAND ----------

# PySpark Inner Join DataFrame

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left") \
     .show(truncate=False)


# COMMAND ----------

# PySpark Inner Join DataFrame

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
     .show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### inner : returns only rows that match in both DataFrames.
# MAGIC ### left / leftouter: returns all rows from the left DataFrame and matching rows from the right; unmatched right side is null.
# MAGIC ### right / rightouter: returns all rows from the right DataFrame and matching rows from the left; unmatched left side is null.
# MAGIC ### full / outer / fullouter: returns all rows from both DataFrames; unmatched side is null.
# MAGIC ### leftsemi: returns only left rows that have a match in the right, with left columns only.
# MAGIC ###leftanti: returns only left rows that have no match in the right, with left columns only

# COMMAND ----------

#PySpark Full Outer Join

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)



# COMMAND ----------

#PySpark Left Outer Join

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left")\
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter")\
    .show(truncate=False)


# COMMAND ----------


empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
   .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
   .show(truncate=False)


# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti") \
   .show(truncate=False)

# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi") \
   .show(truncate=False)

# COMMAND ----------


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('pysparkseries').getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)
  
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
    .show(truncate=False)
    
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter") \
   .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
   .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
   .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi") \
   .show(truncate=False)
   
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti") \
   .show(truncate=False)
   


empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")
   
joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

