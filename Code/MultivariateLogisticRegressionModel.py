# Databricks notebook source

# COMMAND ----------

df = proportionate_indices_representation.toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Y is separated to be the binary recovery flag and X contains all other predictor variables, there is a boolean on whether or not to include waiting times and number of treatments

# COMMAND ----------

if include_treatment_factors ==False:
  del df['six_or_more_treatment_sessions']
  del df['six_week_wait']
  
# we want the x variables being trained on to not contain the recovery flag, or else we will have a perfect model
X = df.drop(['binary_recovery'], 1)
  

# the variable we are trying to predict
Y = df['binary_recovery']


# COMMAND ----------

# the stratify parameter ensures that the indices of economic deprivation are represented in the same proportions found in randomly sampled data set which had stratified sampling according to the proportions found in the unsampled data set 
X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state = 42, stratify= df["indices_converted"])


# COMMAND ----------

scaler = MinMaxScaler()

# for all the columns in X_train, normalise to a scale of 0 to 1
X_train[X_train.columns] = scaler.fit_transform(X_train[X_train.columns])

#just use transform for this, or else you overfit for the test set
X_test[X_test.columns] = scaler.transform(X_test[X_test.columns])

column_headers = X_test.columns.tolist()

# COMMAND ----------

#fits the multivariate logistic regression model

log_reg = sm.Logit(y_train, X_train).fit()

# COMMAND ----------

#prints the model summary

print(log_reg.summary())


# COMMAND ----------

# get predictions from the test data set

y_predicted = log_reg.predict(X_test)

prediction = list((y_predicted.round()))


# COMMAND ----------

# confusion matrix
cm = confusion_matrix(y_test, prediction) 
print ("Confusion Matrix : \n", cm) 
  
# accuracy score of the model
print('Test accuracy = ', accuracy_score(y_test, prediction))

# COMMAND ----------

# get the remaining model assessment metrics

precision, recall, accuracy, f1_score = output_model_assessment(cm)

print("precision", precision, "recall", recall, "accuracy", accuracy,"f1", f1_score)
