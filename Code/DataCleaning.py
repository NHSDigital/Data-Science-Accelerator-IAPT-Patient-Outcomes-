# Databricks notebook source
# MAGIC %run ./SharedFunctions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This cell executes the join between the three tables needed for analysis

# COMMAND ----------

#the table names used to run the pipeline have been redacted

dataframe = sqlContext.sql("SELECT * FROM referral_data a left join master_patient_index b on a.LocalPatientID = b.LocalPatientID left join employment_data c on c.LocalPatientID = b.LocalPatientID WHERE (a.ServDischDate BETWEEN '2021-01-01' AND '2021-12-31')")

completed_treatment_and_caseness_df = dataframe.filter((col("CompletedTreatment_Flag")=='true')& (col("Caseness_Flag")=='true'))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This cell filters the original results to only be those from the organisations with the best economic data quality as derived from the data quality dashboard

# COMMAND ----------

# these are the orgs with 85%+ completeness for economic data quality over the time period as derived from the IAPT data quality dashboard
#https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub/data-quality/iapt-data-quality-dashboard#:~:text=Data%20quality%20dashboard-,Improving%20Access%20to%20Psychological%20Therapies%20(IAPT)%20%E2%80%93%20Data%20Quality%20Dashboard,Therapies%20(IAPT)%20data%20set.&text=This%20dashboard%20presents%20information%20about,submitted%20to%20IAPT%20each%20month.
top_providers = ["AM601", "TAF90", "TAF88", "AKR01", "DA1", "NWX08", "AM7", "ANV01", "RW5ME", "RDYLK", "RDYDL", "RAT", "NMQ", "RWX", "AME01"]

filtered_econ_data_quality = completed_treatment_and_caseness_df.filter(col("b.OrgID_Provider").isin(top_providers))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This cell calculates the days waited between a referral and the first treatment day, and then adds a flag to show if the patient was treated within 6 weeks which is IAPT's target

# COMMAND ----------

#number of days passed from the first referral to the person's first date of treatement is added to the model

filtered_econ_data_quality_waiting_times= filtered_econ_data_quality.withColumn("referral_null", col("ReferralRequestReceivedDate").isNull()).withColumn("treatment_null", col("TherapySession_FirstDate").isNull()) 

filtered_econ_data_quality_waiting_times = filtered_econ_data_quality_waiting_times.withColumn("days_passed", days_passed_waiting("ReferralRequestReceivedDate", "TherapySession_FirstDate", "referral_null", "TherapySession_FirstDate"))

#6 week wait is target treatment window, so this is added as a predictor variable
filtered_econ_data_quality_waiting_times = filtered_econ_data_quality_waiting_times.withColumn("six_week_wait", when(col("days_passed")<=42, 1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC This cell adds flags for the demographic characteristics we want the option to filter by - age, ethnicity, gender, primary presenting symptom

# COMMAND ----------

recovery_and_compliant_with_indices = filtered_econ_data_quality_waiting_times.select("b.IndicesOfDeprivationDecile", "a.PrimaryPresentingComplaint", "a.Recovery_Flag", "c.EmployStatus", "c.JSAInd" , "c.UCInd", "c.BenefitRecInd", "c.SSPInd", "c.SelfEmployInd", "c.WeekHoursWorked", "six_week_wait", "a.CareContact_Count",col("b.Age_RP_StartDate").alias("age"), "Gender", "EthnicCategory")


# COMMAND ----------

ethnicity_conditions=(when((col("EthnicCategory")=="A")|(col("EthnicCategory")=="B")|(col("EthnicCategory")=="C"), "White").when((col("EthnicCategory")=="D")|(col("EthnicCategory")=="E")|(col("EthnicCategory")=="F")|(col("EthnicCategory")=="G"), "Mixed").when((col("EthnicCategory")=="H")|(col("EthnicCategory")=="L")|(col("EthnicCategory")=="J")|(col("EthnicCategory")=="K"), "Asian").when((col("EthnicCategory")=="M")|(col("EthnicCategory")=="N")|(col("EthnicCategory")=="P"), "Black").when((col("EthnicCategory")=="R")|(col("EthnicCategory")=="S"), "Other").when((col("EthnicCategory")=="Z")|(col("EthnicCategory")=="99"), "Not_known").otherwise("Not_known"))

gender_groups=(when(col("Gender")==1, "Male").when(col("Gender")==2, "Female").otherwise("Not_known"))

age_grouping_conditions=(when(col("age")<18, "<18").when((col("age")>18)&(col("age")<=30), "18-30").when((col("age")>30)&(col("age")<=50), "30-50").when((col("age")>50)&(col("age")<=70), "50-70").when(col("age")>70, "+70").otherwise("Not_known"))

#adds the necessary demographic groupings and removes those without a deprivation indice value
recovery_and_compliant_with_indices_and_demographic_info = recovery_and_compliant_with_indices.withColumn("Ethnicity", ethnicity_conditions).withColumn("gender_groups", gender_groups).withColumn("age_groups", age_grouping_conditions).filter(col("IndicesOfDeprivationDecile").isNull()!=True)    

# COMMAND ----------

# MAGIC %md
# MAGIC 

# COMMAND ----------

treatment_recovery_hours_worked_cleaned = recovery_and_compliant_with_indices_and_demographic_info.withColumn("WeekHoursWorkedCleaned", clean_week_hours_worked("c.WeekHoursWorked", "c.EmployStatus"))

treatment_recovery_hours_worked_cleaned_and_employ_status = treatment_recovery_hours_worked_cleaned.na.fill(value="01", subset=["EmployStatus"]).withColumn("EmployStatusNoNulls", regexp_replace("EmployStatus", "ZZ", "01"))


# COMMAND ----------

treatment_recovery_with_recovery_flag = add_binary_recovery_flag(treatment_recovery_hours_worked_cleaned_and_employ_status)

treatment_recovery_with_converted_indices = convert_indices(treatment_recovery_with_recovery_flag)

treatment_with_full_time_employ_flag = full_time_employment_flag(treatment_recovery_with_converted_indices)

treatment_with_unemploy_flag = unemploy_flag(treatment_with_full_time_employ_flag)

treatment_with_self_employ = self_employ_flag(treatment_with_unemploy_flag)

treatment_with_sickpay = sick_pay_flag(treatment_with_self_employ)

treatment_with_benefits = benefits_flag(treatment_with_sickpay)

treatment_with_part_time = part_time(treatment_with_benefits)

treatment_with_job_seeker = job_seeker_flag(treatment_with_part_time)

treatment_with_treatment_num = treatment_num_flag(treatment_with_job_seeker)

treatment_recovery = treatment_with_treatment_num

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The relevant predictor variables are selected for the model

# COMMAND ----------

cleaned_economic_and_demographic_IAPT_data = treatment_recovery.select("binary_recovery","indices_converted","binary_full_time_employment_status","binary_unemployment_status","binary_benefits_status",  "binary_job_seeker_allowance_status", "binary_selfemployment_status", "binary_sickpay_status", "six_or_more_treatment_sessions", "six_week_wait")

# COMMAND ----------


# this cell filters to the demographic information selected for the training run, which is fed through as variables in a different file

if gender!="All":
  treatment_recovery = treatment_recovery.filter(col("gender_groups")==gender)
if ethnicity!="All":
  treatment_recovery = treatment_recovery.filter(col("Ethnicity")==ethnicity)
if age!="All":
  treatment_recovery = treatment_recovery.filter(col("age_groups")==age)
  

if primary_presenting_symptom!="All":
  if primary_presenting_symptom=="Anxiety and stress disorders":
    treatment_recovery = treatment_recovery.filter(col("PrimaryPresentingComplaint")=="Anxiety and stress related disorders (Total)")
  else:
    treatment_recovery = treatment_recovery.filter(col("PrimaryPresentingComplaint")==primary_presenting_symptom)


# COMMAND ----------

# this gives a stratified sample of the filtered data
proportionate_indices_representation= get_stratified_sample(cleaned_economic_and_demographic_IAPT_data)

# COMMAND ----------


