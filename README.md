# Pinterest Data Pipeline

## Table of Contents

- [Description](#description)
- [Tools and Technologies](#tools-and-technologies)
- [Pipeline Architecture](#pipeline-architecture)
- [Data and database](#data-and-database)
- [File Structure](#file-structure)
- [License](#license)


## Description
In this project, commissioned by Aicore, I constructed two distinct pipelines – batch processing and streaming processing – with the aim of facilitating seamless data collection and transformation. Embarking on my inaugural endeavor into an end-to-end data pipeline project, I encountered a multitude of challenges. These hurdles ranged from familiarizing myself with novel technologies in AWS (including API Proxy Integrations, Kafka, Kinesis, MWAA) and Databricks, to navigating Design Architecture intricacies, code refactoring, and overcoming obstacles within the pipeline processes. These emerging technologies serve as the foundational elements for building scalable and efficient pipelines. Throughout this journey, my passion for data manipulation was reignited, further bolstering my resolve to achieve greater heights through unwavering dedication. 

A comprehensive report detailing the events and challenges encountered during the development process can be accessed through the following link: [report].

## Technologies
### Python Libraries
- `sqlalchemy`:
- `requests:`
- `urllib`:
- `random`:
- `time`:
- `datetime`:
- `json`:
- `airflow`:
- `pyspark.sql.functions`:
- `pyspark.sql.types`:

### AWS Services
- #### S3(Simple Storage Service):
- #### Amazon RDS :
- #### EC2(Elastic Compute Cloud):
- #### API Gateway:
- #### IAM(Identity and Access Management):
- #### MSK(Managed Streaming for Apache Kafka):
- #### MWAA(Managed Workflows for Apache Airflow):
- #### Kinesis:

### Databricks
 - #### Notebooks:
 - #### Delta Tables:

 ## Pipeline Architecture
 For this project, I developed two distinct data pipelines. One was designed for Batch Processing, utilizing an ELT (Extract, Load, Transform) pipeline, while the other was tailored for Streaming, employing an ETL (Extract, Transform, Load) pipeline. In the following sections, I will delve into their respective architectures, accompanied by provided diagrams.

 ### Batch Processing Architecture






















