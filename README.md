# IAPT Patient Outcomes

##### This repository is maintained by the NHS Digital Data Science team.

##### To contact us raise an issue on Github or via our email (datascience@nhs.net) and we will respond promptly.

See our other work here: [NHS Digital Analytical Services](https://github.com/NHSDigital/data-analytics-services).


## Project motivation

This project was undertaken as a part of the data science accelerator programme from April - July 2022. More information can be found at this page: https://www.gov.uk/government/publications/data-science-accelerator-programme/introduction-to-the-data-science-accelerator-programme 

The aim of the project is to try and understand and identify confounding variables which might affect a patient’s access to psychological therapies and/or their likelihood of responding positively to treatment. Current research may have identified some demographic characteristics associated with the quality of an individual’s experience with a mental health service, such as longer waiting times for certain demographic groups. However, further analysis may be warranted to determine a wide range of socio-economic factors  which could be affecting patient outcomes and may not be identifiable through analysis of the IAPT dataset in isolation. 

The IAPT dataset is centered around three key aspects of patient experience: access to services, waiting times and patient outcomes, the latter being judged from a patient assessment before and after treatment. The project identifies new relationships and reasons for variable quality of access to, and outcomes from, these particular mental health services. 

To find out more about the IAPT data set visit this page: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/improving-access-to-psychological-therapies-data-set/improving-access-to-psychological-therapies-data-set-reports

To gain access to the IAPT data you can request access to the trusted research environment (TRE): https://digital.nhs.uk/services/data-access-request-service-dars/dars-products-and-services 

### Project output

The project centres around multiple logistic regression analysis, which aims to predict patient recovery based on a variety of different socio-economic predictor variables. From here, the model should identify patient characteristics which are more closely associated with positive or negative outcomes and give novel insight into correlations between socio-economic factors and how these might lead to variable patient experiences with these psychological services. 

To support subsequent re-use of this work and broader skills development, this work will be shared through an internal seminar and, where appropriate, open release of the codebase in line with NHSD governance processes.


## Requirements

This code needs to be run in Databricks version 3.60 with Spark version 2.4.5 and the following packages:

Packages used in the project:
 - Pyspark
 - Sklearn
 - Datetime
 - Pandas
 - Numpy
 - Math
 - Functools
 - Statsmodels
 - unittest


## To run the pipeline

- You need to define the table of interest as 'dataframe' in the 'DataCleaning.py' file. Original table names have been redacted.
- Running the 'PopulationSegmentedTableGenerate.py' file will automatically run all other files so long as they are in the same directory level
- The field names for demographic features in the 'PopulationSegmentedTableGenerate.py' file will need to be renamed to match the equivalent names in the new reference data set
- The field names for the predictor variables of interest in 'DataCleaning.py' will need to renamed and redefined for the new reference data set
- There is a folder called 'Tests' which allows the functions which clean the data to be tested
- The final product will be the logistic regression model produced in the 'MultivariateLogisticRegressionModel.py' file

### Licence 
IAPT patient outcomes codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the Open Government 3.0 licence.

https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/

