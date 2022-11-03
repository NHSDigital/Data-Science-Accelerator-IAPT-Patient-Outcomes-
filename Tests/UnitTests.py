# Databricks notebook source
# MAGIC %run ./TestSuite

# COMMAND ----------

# MAGIC %run ./SharedFunctions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

binary_recovery_suite =  FunctionTestSuite()

@binary_recovery_suite.add_test
def test_recovery_flag():
  
  df_input = spark.createDataFrame(
  [
    (1, True),
    (3, True),
    (2, None)
  ], 
  ["id", "Recovery_Flag"]
  )
  
  output_schema = StructType([
  StructField('id', StringType(), True),
  StructField('Recovery_Flag', BooleanType(), True),
  StructField('binary_recovery', IntegerType(), False)
  ])  
  
  df_expected = spark.createDataFrame(
  [
    (1, True, 1),
    (3, True, 1), 
    (2, None, 0)
  ], 
  ["id", "Recovery_Flag", "binary_recovery"], StructType(output_schema)
  )
  df_expected=df_expected.withColumn("binary_recovery", col("binary_recovery").cast(IntegerType()))
  
  df_output = add_binary_recovery_flag(df_input)
  
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
binary_recovery_suite.run()

# COMMAND ----------

full_time_employment_test_suite =  FunctionTestSuite()

@full_time_employment_test_suite.add_test
def test_employ_flag_full():
  
  df_input = spark.createDataFrame(
  [
    (1, "01", "01"),
    (3, "01", "01"),
    (2, "01", "01")
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned"]
  )
  
  
  df_expected = spark.createDataFrame(
  [
    (1, "01", "01", 1),
    (3, "01", "01", 1),
    (2, "01", "01", 1)
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned", "binary_full_time_employment_status"]
  )
  
  df_expected=df_expected.withColumn("binary_full_time_employment_status", col("binary_full_time_employment_status").cast(IntegerType()))
  df_output = full_time_employment_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns=['id'])
  

@full_time_employment_test_suite.add_test
def test_employ_flag_mixed():
  
  df_input = spark.createDataFrame(
  [
    (1, "02", "01"),
    (3, "01", "98"),
    (2, "02", "01")
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned"]
  )
  
  
  df_expected = spark.createDataFrame(
  [
    (1, "02", "01", 1),
    (3, "01", "98", 1),
    (2, "02", "01", 1)
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned", "binary_full_time_employment_status"]
  )
  
  df_expected=df_expected.withColumn("binary_full_time_employment_status", col("binary_full_time_employment_status").cast(IntegerType()))
  df_output = full_time_employment_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
@full_time_employment_test_suite.add_test
def test_employ_flag_unemploy():
  
  df_input = spark.createDataFrame(
  [
    (1, "02", "98"),
    (3, "04", "98"),
    (2, "02", "04")
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned"]
  )
  
  
  df_expected = spark.createDataFrame(
  [
    (1, "02", "98", 0),
    (3, "04", "98", 0),
    (2, "02", "04", 0)
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned", "binary_full_time_employment_status"]
  )
  
  df_expected=df_expected.withColumn("binary_full_time_employment_status", col("binary_full_time_employment_status").cast(IntegerType()))
  df_output = full_time_employment_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
full_time_employment_test_suite.run()

# COMMAND ----------

unemployment_test_suite =  FunctionTestSuite()

@unemployment_test_suite.add_test
def test_employ_flag_full():
  
  df_input = spark.createDataFrame(
  [
    (1, "01", "01"),
    (3, "01", "01"),
    (2, "01", "01")
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned"]
  )
  
  
  df_expected = spark.createDataFrame(
  [
    (1, "01", "01", 0),
    (3, "01", "01", 0),
    (2, "01", "01", 0)
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned", "binary_unemployment_status"]
  )
  
  df_expected=df_expected.withColumn("binary_unemployment_status", col("binary_unemployment_status").cast(IntegerType()))
  df_output = unemploy_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns=['id'])
  

@unemployment_test_suite.add_test
def test_employ_flag_mixed():
  
  df_input = spark.createDataFrame(
  [
    (1, "02", "01"),
    (3, "01", "98"),
    (2, "02", "01")
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned"]
  )
  
  
  df_expected = spark.createDataFrame(
  [
    (1, "02", "01", 1),
    (3, "01", "98", 1),
    (2, "02", "01", 1)
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned", "binary_unemployment_status"]
  )
  
  df_expected=df_expected.withColumn("binary_unemployment_status", col("binary_unemployment_status").cast(IntegerType()))
  df_output = unemploy_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
@unemployment_test_suite.add_test
def test_employ_flag_unemploy():
  
  df_input = spark.createDataFrame(
  [
    (1, "02", "98"),
    (3, "04", "98"),
    (2, "02", "04")
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned"]
  )
  
  
  df_expected = spark.createDataFrame(
  [
    (1, "02", "98", 1),
    (3, "04", "98", 1),
    (2, "02", "04", 1)
  ], 
  ["id", "EmployStatusNoNulls", "WeekHoursWorkedCleaned", "binary_unemployment_status"]
  )
  
  df_expected=df_expected.withColumn("binary_unemployment_status", col("binary_unemployment_status").cast(IntegerType()))
  df_output = unemploy_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
unemployment_test_suite.run()

# COMMAND ----------

benefits_sick_pay_suite =  FunctionTestSuite()

@benefits_sick_pay_suite.add_test
def test_sick_pay_flag():
  
  df_input = spark.createDataFrame(
  [
    (1, "Y", "04"),
    (3, "N", "04"),
    (2, "Y", "01"),
    (4,"N","01")
  ], 
  ["id", "SSPInd", "EmployStatusNoNulls"]
  )
  
  df_expected = spark.createDataFrame(
  [
    (1, "Y", "04", 1),
    (3, "N", "04", 1),
    (2, "Y", "01", 1),
    (4,"N","01", 0)
  ], 
  ["id", "SSPInd", "EmployStatusNoNulls", "binary_sickpay_status"]
  )
  df_expected=df_expected.withColumn("binary_sickpay_status", col("binary_sickpay_status").cast(IntegerType()))
  
  df_output = sick_pay_flag(df_input)
  
  assert compare_results(df_output, df_expected, join_columns=['id'])

@benefits_sick_pay_suite.add_test
def test_benefits_flag():
  
  df_input = spark.createDataFrame(
  [
    (1, "Y", "Y"),
    (3, "N", "Y"),
    (2, "Y", "N"),
    (4,"N","N")
  ], 
  ["id", "BenefitRecInd", "UCInd"]
  )
  
  df_expected = spark.createDataFrame(
  [
    (1, "Y", "Y", 1),
    (3, "N", "Y", 1),
    (2, "Y", "N", 1),
    (4,"N","N", 0)
  ], 
  ["id", "BenefitRecInd", "UCInd", "binary_benefits_status"]
  )
  df_expected=df_expected.withColumn("binary_benefits_status", col("binary_benefits_status").cast(IntegerType()))
  
  df_output = benefits_flag(df_input)
  
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
@benefits_sick_pay_suite.add_test
def test_jsa_flag():
  
  df_input = spark.createDataFrame(
  [
    (1, "Y"),
    (3, "N")
  ], 
  ["id", "JSAInd"]
  )
  
  df_expected = spark.createDataFrame(
  [
    (1, "Y", 1),
    (3, "N", 0)
  ], 
  ["id", "JSAInd", "binary_job_seeker_allowance_status"]
  )
  df_expected=df_expected.withColumn("binary_job_seeker_allowance_status", col("binary_job_seeker_allowance_status").cast(IntegerType()))
  
  df_output = job_seeker_flag(df_input)
  
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
benefits_sick_pay_suite.run()

# COMMAND ----------

employment_types_tests =  FunctionTestSuite()

@employment_types_tests.add_test
def test_recovery_flag():
  
  df_input = spark.createDataFrame(
  [
    (1, "part_time"),
    (3, "01"),
    (2, "part_time")
  ], 
  ["id", "WeekHoursWorkedCleaned"]
  )
  df_expected = spark.createDataFrame(
  [
    (1, "part_time", 1),
    (3, "01", 0),
    (2, "part_time", 1)
  ], 
  ["id", "WeekHoursWorkedCleaned", "binary_partime_status"]
  )
  df_expected=df_expected.withColumn("binary_partime_status", col("binary_partime_status").cast(IntegerType()))
  
  df_output = part_time(df_input)
  
  assert compare_results(df_output, df_expected, join_columns=['id'])
  
@employment_types_tests.add_test
def self_employment_test():
  
  df_input = spark.createDataFrame(
  [
    (1, "Y"),
    (3, "N"),
    (2, "0Z")
  ], 
  ["id", "SelfEmployInd"]
  )
  df_expected = spark.createDataFrame(
  [
    (1, "Y", 1),
    (3, "N", 0),
    (2, "0Z", 0)
  ], 
  ["id", "SelfEmployInd", "binary_selfemployment_status"]
  )
  df_expected=df_expected.withColumn("binary_selfemployment_status", col("binary_selfemployment_status").cast(IntegerType()))
  
  df_output = self_employ_flag(df_input)
  
  assert compare_results(df_output, df_expected, join_columns=['id'])
    
employment_types_tests.run()
