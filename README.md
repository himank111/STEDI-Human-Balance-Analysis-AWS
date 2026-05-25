# STEDI-Human-Balance-Analysis-AWS
AWS Lakehouse Solution used to transform raw data collect from website, sensor data and mobile phones into curated data used for machine learning.

# Project Overview

The project aims at making use of data collected from STEDI sensor data, mobile phones and apply machine learning analysis. This is achieved by making a use of AWS Lakehouse Soltions to process the raw data from the landing zone and curate them to be available machine learning processing.

<img width="897" height="307" alt="image" src="https://github.com/user-attachments/assets/77d04da3-8483-4e0f-b41a-35d16212c280" />

# Methodology
The Technology Stack used for the project include

* AWS S3
* AWS Athena
* AWS Glue
* Python and Spark

The raw data exists in a semi structured format and is collected and consolidated into 3 data sources

* Customer
* Accelerometer
* Step Trainer

To process the data in the landing zone and transform them into curated we made use of Glue Jobs

[1. Customer Landing to Trusted](customer_landing_to_trusted.py)

Customer data in the landing zone was filtered using a Privacy Filter to remove those user who did not consent to the research

[2. Accelerometer Landing to Trusted](accelerometer_landing_to_trusted.py)

Accelerometer data in the landing zone was filtered to include accelerometer data for only those users who had consented towards the research

[3. Customer Trusted to Curated](customer_trusted_to_curated.py)

Customer data in the trusted zone had to be sanitised to include only those customers who consented to the research and had accelerometer data available for them

[4. Step Trainer Landing to Trusted](step_trainer_landing_to_trusted.py)

Step Trainer data in the landing zone was filtered to keep Step Trainer data for only those customer who had consented to the research and have accelerometer data

[5. Machine Learning Curated](machine_learning_curated.py)

An aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data

