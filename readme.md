# ETL enriched US national park trails data to GBQ

## Table of Contents

- [Project Name](#project-name)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Features](#features)
  - [Installation](#installation)

## Description

The purpose of this data pipeline is to enrich a US national park trails data set with climate data and bring it into Google Cloud Platform (GCP). 

Some examples of potential consumer use cases include: 
 - Park administrators can examine trails to identify alternative uses. This may be used to inform park communication on trail regulations.
 - Adventurers can use the data to identify travel destination opportunities based on climate preferences.

## Features

  - Raw data is dropped in Google Cloud Storage (GCS) bucket. Example data is provided with this project. Acknowleding source data: 
    - Alltrails: https://github.com/j-ane/trail-data
    - Average Temperatures by State 2024: https://worldpopulationreview.com 
  - Data is processed using PySpark and temporarily staged in GCS bucket before it's placed in Google BigQuery (GBQ) table.
  - A Directed Acyclic Graph (alltrails_dag.py provided) can be used to orchestrate the data pipeline. This includes checking for the presence of raw data sources, creating/deleting the cluster and executing the data processing.

## Installation

To run in GCP you will need to have a project with the following enabled and stood up:
 - GCS: Create a bucket to store your raw data and alltrails_etl.py-generated staging data.
 - GBQ: Designate a dataset and table name in the alltrails_etl.py code
 - Cloud Composer: Create a present small Cloud Composer 2 environment. Ensure that the service account has editor permissions to Dataproc so it can create/delete cluster resources. 
 - Dataproc: Enable API so Cloud Composer service account may access it. Please note, the project's DAG requires 2 worker nodes which exceeds current GCP Free Trial quota for Dataproc clusters.
 - Customize the PLACEHOLDER environmental variables in alltrails_etl.py and alltrails_dag.py for your use.
