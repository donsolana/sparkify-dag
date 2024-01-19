# Sparkify-dag
Data pipeline for music streaming platform with Apache airflow


## Introduction 

This project contains a pipeline for a streaming platform. It is an automated version of the [Red-Hot](https://github.com/donsolana/Red-Hot) project in another repository in this account. 

## Structure
#### 1. Dag: Contains the main **Directed Acyclic Graph(DAG)** definition.
#### 2. Plugins: Contains definitions of custom Airflow Operators and helper files

I. ***helper files*** 
    a. *Init file*: This is part of airflow's standard structure when using helper scripts, and allows the script to be called upon in the dag definition.
    b. *SQL queries*: This document contains SQL insert statements for the Redshift warehouse.

II. ***Operators***: Operators are re-useable elements in the dags, they are designed as Python classes that inherit properties of a base Airflow class.


   


