# Databricks notebook source
dbutils.widgets.dropdown("Primary Presenting Symptom", "All", ["All", "Depression" , "Anxiety and stress disorders", "Unspecified", "Invalid Data supplied", "Other"])

dbutils.widgets.dropdown("Gender", "All", ["All", "Male" , "Female"])

dbutils.widgets.dropdown("Age", "All", ["All", "<18" , "18-30", "30-50",  "50-70", "+70"])

dbutils.widgets.dropdown("Ethnicity", "All", ["All", "White" , "Mixed", "Asian", "Black", "Other", "Not_known"])

# COMMAND ----------

# these are the features we are filtering the dataset by

primary_presenting_symptom = dbutils.widgets.get("Primary Presenting Symptom")

gender = dbutils.widgets.get("Gender")

ethnicity = dbutils.widgets.get("Ethnicity")

age = dbutils.widgets.get("Age")

print("Primary symptom: ", primary_presenting_symptom, ", Gender: ", gender, ", Ethnicity: ",ethnicity, ", Age: ", age) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The below file cleans the data and filters to produce a stratified sample of 500,000 with the chosen symptom, gender, ethnicity and age. 

# COMMAND ----------

# MAGIC   %run ./DataCleaning

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This code block attempts to load a table if one has been previously generated, or create one if the sample has not been generated previously.

# COMMAND ----------
#first part of the table name has been redacted

table_name = "random_sample"+"_symptom_"+primary_presenting_symptom+"_gender_"+gender+"_ethnicity_"+ethnicity+"_age_"+age

table_name = table_name.replace(" ", "_")

try:
  proportionate_indices_representation = spark.table(table_name)
  

except:
  # create a permanent table for the population sample
  # the proportionate indices representation is the table outputted from data cleaning file
  create_table(proportionate_indices_representation, table_name, table=None, overwrite=True)

  print("Table "+table_name+" has been created")

else:
  print("Table "+table_name+" has been loaded")  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This boolean removes waiting times and number of treatments flag. This model will be trained with the treatment variables.

# COMMAND ----------


proportionate_indices_representation = spark.table(table_name)

include_treatment_factors = False

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This file generates the multivariate logistic regression model excluding treatment variables.

# COMMAND ----------

# MAGIC %run ./MultivariateLogisticRegressionModel $proportionate_indices_representation = proportionate_indices_representation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This boolean includes waiting times and number of treatments flag. This model will be trained with the treatment variables.

# COMMAND ----------

proportionate_indices_representation = spark.table(table_name)

include_treatment_factors = True

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This file generates the multivariate logistic regression model including treatment variables.

# COMMAND ----------

# MAGIC %run ./MultivariateLogisticRegressionModel $proportionate_indices_representation = proportionate_indices_representation
