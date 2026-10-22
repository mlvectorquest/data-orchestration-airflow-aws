# Data Lakehouse Project

Starter code for the course project: an Airflow-orchestrated data lakehouse on AWS (S3, Glue) with raw, transactions, and analytics layers.

## Environment

> **This project is not intended to be run in a local development environment.**

The pipeline depends on AWS services, Airflow, and datasets that are pre-provisioned for you. Complete and run the project in the **Udacity Workspace**, which comes configured with everything you need. Cloning and running this repository on your own machine is not supported.

## Getting Started

Follow the instructions in the classroom to set up the AWS resources using the provided CloudFormation template. Airflow in the workspace loads DAGs from this directory automatically — complete the `#### YOUR CODE HERE` sections in the DAG files.

## Project structure

- `setup/` — DAGs to bootstrap project data and run the pipeline
- `raw/` — raw ingestion DAG and Glue script
- `transactions/` — transactions layer DAG, Glue script, and SQL
- `analytics/` — analytics layer DAG, Glue script, and SQL
- `lakehouse_infrastructure.yml` — CloudFormation template for the AWS resources

## License

[License](LICENSE.txt)
