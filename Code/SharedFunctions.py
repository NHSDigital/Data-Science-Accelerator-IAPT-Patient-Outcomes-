# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import RFE
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score

from datetime import datetime
import pandas as pd
import numpy as np
import math
from functools import reduce
import patsy
import statsmodels.api as sm

# COMMAND ----------

#takes stratified sample according to the original indices proportion found in the original data set
def get_stratified_sample(df):
  random_samples = []
  union_df = None 
  # the indices fractions should be set to the proportion with which they are found in the data set - have been set to default values assuming an even distribution so set these to whatever proportion you find indices within the data
  indices_fractions={1:0.1, 2:0.1, 3:0.1, 4:0.1, 5:0.1, 6:0.1, 7:0.1, 8:0.1, 9:0.1, 10:0.1}
  
  for i in range(1,11):
    indices_df = df.filter(col("indices_converted")==str(i))
    # set a seed on the rand function 
    sample_size = int(500000*indices_fractions[i])
    # 500,000 x fraction with which found in the data gives stratified representation -this is the optimal number to sample for pandas conversion
    random_sample = indices_df.orderBy(rand(seed=42)).limit(sample_size)
    random_samples.append(random_sample)
      
  union_df = reduce(DataFrame.unionAll, random_samples)    
  return union_df

# COMMAND ----------

def sigmoid(x):
  return 1 / (1 + math.exp(-x))


def prediction_function(indice):
    z = model.coef_ * indice + model.intercept_ 
    y = sigmoid(z)
    return y

# COMMAND ----------

def output_model_assessment(cf_matrix):
  total_observations =  np.sum(cf_matrix)
  #false negative for incorrectly predicted no-event values.
  false_negative_rate = cf_matrix[0][1]

  #true negative for correctly predicted no-event values
  true_negative_rate = cf_matrix[0][0]

  #false positive for incorrectly predicted event values.
  false_positive_rate = cf_matrix[1][0]

  #true positive for correctly predicted event values
  true_positive_rate = cf_matrix[1][1]
  
  #Precision = TruePositives / (TruePositives + FalsePositives)
  precision = true_positive_rate/(true_positive_rate+false_positive_rate)  

  #Recall = TruePositives / (TruePositives + FalseNegatives)
  recall = true_positive_rate/(true_positive_rate+false_negative_rate)  

  #accuracy = true positive + true negative / (true positive + false positive + true negative + false negative)
  accuracy = (true_positive_rate+true_negative_rate)/(total_observations)  

  f1_score = 2*((precision*recall)/(precision+recall))

  return precision, recall, accuracy, f1_score

# COMMAND ----------

@udf("string")
def clean_week_hours_worked(week_hours, employ_status):     
  # can assume anyone full time is doing 30+ hours
  if employ_status=="01":
      return "01"
  # have seen contradictions, but will assume anyone not employed is doing zero hours
  elif employ_status=="02":
      return "98"
  # can assume anyone retired is not working
  elif employ_status=="08":
      return "98"
    
  elif week_hours=="02" or week_hours=="03" or week_hours=="04":
    return "part_time"
  
  elif week_hours==None:
      return "99"
    
  else: 
    return week_hours

# COMMAND ----------

# calculates the time spent waiting for referral by the difference in referral date to day of first treatment
def days_between(d1, d2):
    if d1==None or d2==None:
      return "hello"
    else:
      d1 = datetime.strptime(str(d1), "%Y-%m-%d")
      d2 = datetime.strptime(str(d2), "%Y-%m-%d")
      return (d2 - d1).days

@udf("integer")
def days_passed_waiting(referral, treatment, ref_null, tre_null):
  if ref_null==True:
    return None
  elif tre_null==True:
    return None
  else:
    days_passed = days_between(referral, treatment)
    return days_passed

# COMMAND ----------

def output_model_results(column_headers, model, y_test, y_predicted):
  print("Coefficient values for the model:", '\n')
  
  for i in range(0,len(column_headers)):
    print(column_headers[i],model.coef_[0][i] )

  #Generate the confusion matrix
  cf_matrix = confusion_matrix(y_test, y_predicted)
  
  print('\n', "Confusion matrix:", '\n')
  
  print(pd.crosstab(y_test, y_predicted, rownames = ['Actual'], colnames =['Predicted'], margins = True))

# COMMAND ----------


def threshold_testing(Y_test, X_test):
  #https://stackoverflow.com/questions/28716241/controlling-the-threshold-in-logistic-regression-in-scikit-learn
  #testing different threshold levels for the model
  pred_proba_df = pd.DataFrame(model.predict_proba(X_test))
  threshold_list = [0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55,0.6,0.65,.7,.75,.8,.85,.9,.95,.99]

  for i in threshold_list:
      print ('\n******** For i = {} ******'.format(i))
      y_predicted = []
      predicted_positive_outcomes = pred_proba_df[1].tolist()
      for result in predicted_positive_outcomes:
        if result>i:
          y_predicted.append(1)
        else:
          y_predicted.append(0)      


      cf_matrix = confusion_matrix(y_test, y_predicted)
      print(cf_matrix)

      if 0 not in cf_matrix:
        precision, recall, accuracy, f1_score = output_model_assessment(cf_matrix)
        print("Precision: ", precision,", Recall: ", recall, ", Accuracy: ", accuracy, ", F1 score: ", f1_score)

# COMMAND ----------

def create_table(df, db_or_asset, table=None, overwrite=False):
  
  if table is None:
    asset_name = db_or_asset
    db = db_or_asset.split('.')[0]
    table = db_or_asset.split('.')[1]
  else:
    asset_name = f'{db_or_asset}.{table}'
    db = db_or_asset
 
  if overwrite is True:
    df.write.saveAsTable(asset_name, mode='overwrite')
  else:
    df.write.saveAsTable(asset_name)
      
  if db == 'test_epma_autocoding':
    spark.sql(f'ALTER TABLE {asset_name} OWNER TO `data-managers`')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Below are the functions used to clean the data for the model

# COMMAND ----------

def add_binary_recovery_flag(df):
  """ get the recovery flag as 1 if recovered and 0 if they did not recover """
  
  df = df.withColumn("binary_recovery", when(col("Recovery_Flag")==True, 1).otherwise(0))
  return df

# COMMAND ----------

# COMMAND ----------

def add_binary_recovery_flag(df):
  """ get the recovery flag as 1 if recovered and 0 if they did not recover """
  
  df = df.withColumn("binary_recovery", when(col("Recovery_Flag")==True, 1).otherwise(0))
  return df

def convert_indices(df):
  """ converts the indices of deprivation to integer type """
  df = df.withColumn("indices_converted", df["IndicesOfDeprivationDecile"].cast(IntegerType()))
  return df

def full_time_employment_flag(df):
  """ returns 1 if either in full time employment or doing 30+ hours a week, 0 otherwise """
  df = df.withColumn("binary_full_time_employment_status", when((col("EmployStatusNoNulls")=="01")|(col("WeekHoursWorkedCleaned")=="01"), 1).otherwise(0))
  return df

def unemploy_flag(df):
  """ returns 1 if the patient is not employed, 0 otherwise """
  df = df.withColumn("binary_unemployment_status", when(((col("EmployStatusNoNulls")=="02")|(col("WeekHoursWorkedCleaned")=="98")), 1).otherwise(0))
  return df

def self_employ_flag(df):
  """ returns 1 if patient is self-employed, 0 otherwise """
  df=df.withColumn("binary_selfemployment_status", when(col("SelfEmployInd")=="Y", 1).otherwise(0))
  return df

def sick_pay_flag(df):
  """ returns 1 if patient is long term sick or disabled indicator on employment status, or receiving sick pay, 0 otherwise """
  df = df.withColumn("binary_sickpay_status", when((col("SSPInd")=="Y")|(col("EmployStatusNoNulls")=="04"), 1).otherwise(0))
  return df

def benefits_flag(df):
  """ returns 1 if patient is on benefits or universal credit, 0 otherwise """
  df = df.withColumn("binary_benefits_status", when((col("BenefitRecInd")=="Y")|(col("UCInd")=="Y"), 1).otherwise(0))
  return df

def part_time(df):
  """ return 1 if patient is on part time hours, 0 otherwise """
  df = df.withColumn("binary_partime_status", when(col("WeekHoursWorkedCleaned")=="part_time", 1).otherwise(0))
  return df

def job_seeker_flag(df):
  """ return 1 if patient is on a jobseeker allowance, 0 otherwise """
  df = df.withColumn("binary_job_seeker_allowance_status", when(col("JSAInd")=="Y", 1).otherwise(0))
  return df

def treatment_num_flag(df):
  """ return 1 if patient has receieved 6 or more treatment sessions, 0 otherwise """
  df=df.withColumn("six_or_more_treatment_sessions", when(col("CareContact_Count")>=6, 1).otherwise(0))
  return df
