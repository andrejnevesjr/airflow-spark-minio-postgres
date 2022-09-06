# POC - Playing with Spark + Airflow + MinIO + Postgres

## Project Structure

This project is currently structured with the following specifications.

| Path | Description |
| ------ | ------ |
| src | Contains Airflow Dockerfile and all configurations to setup your environment |

<hr>

##  High Level Architecture

[!some image]

<hr/>

##  Containers

* **airflow-webserver**: Airflow v2.2.4 (Webserver & Scheduler)
    * image: andrejunior/airflow-spark:latest | Based on python:3.8-buster
    * port: 8085 
  
* **postgres**: Postgres database (Airflow metadata and our pipeline)
    * image: postgres:14
    * port: 5432

* **spark-master**: Spark Master
    * image: bitnami/spark:3.2.1
    * port: 8081

* **spark-worker**: Spark workers
    * image: bitnami/spark:3.2.1
  
<hr/>

## Step-by-Step

### 1. Clone the Repository

    $ git clone https://github.com/andrejnevesjr/airflow-spark-minio-postgres.git

### 2. Setup environment

    $ cd airflow-spark-minio-postgres
    $ docker-compose -f docker-compose.yml up -d

### 3. Airflow: Create user for UI
To access Airflow UI is required to create a new user account, so in our case, we are going to create an fictional user with an Admin role attached.

> **NOTE**: Before **RUN** the command below please confirm that Airflow is up and running, it can be checked by accessing the URL [http://localhost:8085](http://localhost:8085). Have in mind that in the first execution it may take 2 to 3 minutes :stuck_out_tongue_winking_eye:

    $ docker-compose run airflow-webserver airflow users create --role Admin --username airflow \
      --email airflow@example.com --firstname airflow --lastname airflow --password airflow

### 3.1 Airflow: Postgres, MinIO & Spark connections configuration

1. Open the service in your browser at http://localhost:8085
   Use the credentials 
   ```
   User: airflow
   Password: airflow
   ```
2. Click on Admin -> Connections in the top bar. 
3. Click on + sign and fill in the necessary details for each source below:

#### Postgres

    Conn Id: postgres_conn
    Conn Type: Postgres
    Host: postgres
    Schema: airflow
    Login: airflow
    Password: airflow
    Port: 5432
 

####   MinIO

    Conn ID: minio_conn
    Conn Type: Amazon S3
    Extra: consists of the JSON below:
```
    { "aws_access_key_id":"airflow",
      "aws_secret_access_key": "airflow",
      "host": "http://bucket:9000"
    }
```
####   Spark

    Conn ID: spark_conn
    Host: spark://spark
    Port: 7077
    Extra: consists of the JSON below:
```
{"queue": "root.default"}
```

<hr/>


## Data Stack

Airflow: <br> 
[http://localhost:8085](http://localhost:8085) 
```
User: airflow
Password: airflow
```
Spark Master: <br>
[http://localhost:8081](http://localhost:8081)  

MinIO: <br> 
[http://localhost:9001](http://localhost:9001) 
```
User: airflow
Password: airflow
```
Postgres: <br> 

```
Server: localhost
Port: 5432
Database: airflow
User: airflow
Password: airflow
```

<hr/>

## References
  - [airflow.apache.org](https://airflow.apache.org/docs/apache-airflow/stable/) 
  - [puckel/docker-airflow](https://github.com/puckel/docker-airflow) 
  - [cordon-thiago/airflow-spark](https://github.com/cordon-thiago/airflow-spark/)
  - [pyjaime/docker-airflow-spark](https://github.com/pyjaime/docker-airflow-spark/)
