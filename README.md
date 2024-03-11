# Smart City

Objective of this project is to smartly monitor a city and for this purpose, we mimic the data extraction from streaming APIs like `vehicle, gps, traffic, weather and emergency` and then produce the data in Kafka topics in real-time and then process the data in
real-time using Spark streaming and store in AWS S3 bucket. After that we run the AWS Glue crawler to extract data from s3 bucket
and store analytics in AWS redshift and which can be connected to PowerBI to give real time analytics dashboard.

# Smart City Pipeline Architecture
![Image Alt Text](images/smart_city_architecture.png)

# How to Setup and Run Project

- Clone this repo in your system and go to `smart-city` directory
```bash 
cd smart-city
```

- Rename `smartcity.env` to `.env` and replace all the required credentials with your respective aws credentials.

- Create a virtual env and activate it and then install required packages, replace `venv_name` with any prefered name
```bash 
python3 -m venv <venv_name>
source <venv_name>/bin/activate
pip install -r requirements.txt
```

- Run Kafka and Spark containers,
```bash 
docker-compose up -d
```

- You can view Spark Cluster UI at `localhost:9090`

- Create an S3 bucket with any given name and attach the following policy with S3 bucket,
```bash 
{
    "Version": "2012-10-17",
    "Id": "Policy1708964691216",
    "Statement": [
        {
            "Sid": "Stmt1708964685647",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": "arn:aws:s3:::smartcity-spark-streaming-data/*"
        }
    ]
}
```

- Make sure the AWS user whose credentials you are using in `config.py` must have S3 bucket access

- Now run the spark job in spark container to process stream data from kafka topics and store it in S3 bucket 
by executing the below command,
```bash 
docker exec -it smart-city-spark-master-1 spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
jobs/spark-city.py
```

- Produce data in the kafka topics by running the below command,
```bash
python3 jobs/main.py
```

- Kafka commands to list and delete the topics, also to view the topics data,
```bash
kafka-topics --list --bootstrap-server broker:29092
kafka-topics --delete --topic gps_data --bootstrap-server broker:29092
kafka-console-consumer --bootstrap-server broker:29092 --topic <topic_name> --from-beginning
```

# ETL Pipeline Architecture
![Image Alt Text](images/etl_architecture.png)

# ETL Pipeline Data Flow
![Image Alt Text](images/etl_dataflow.png)

Current ETL implementation does not have lambda service integrated as shown above in diagram.

## S3-to-S3 ETL
- Now, create an `AWS Glue Crawler` and link the S3 bucket path with it and once you run the crawler, you will be able to query
your data using `AWS Athena`

- Create an ETL in AWS Glue using `glue_jobs/smart_city_read_from_s3.py` script to extract data from s3, transform it and load it into s3.

## S3-to-Redshift ETL
- Now, create an `AWS Glue Crawler` and link it with redshift table and crawl it.

- Create a `redshift cluster` using `redshift/create_redshift_cluster.py` script and create a database in it. Attach the IAM role with AWS s3 access to redshift cluster.

- Create an `AWS Glue connection` with Redshift to crawl redshift table. Test connection by attaching an IAM role with permissions of S3, Glue and Redshift.

- Create an ETL in AWS Glue using `glue_jobs/glue_insert_to_redshift.py` to extract data from s3, transform it and load it into redshift table.


# Resources
- https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-redshift-home.html
- https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html
- https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/build-an-etl-service-pipeline-to-load-data-incrementally-from-amazon-s3-to-amazon-redshift-using-aws-glue.html