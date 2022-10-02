# Data Engineering Pipeline: Schiphol Flights


<!-- ABOUT THE PROJECT -->
## About The Project
This data engineering case was made for [Digital Power](https://digital-power.com/ "Digital Power"). It is an ETL pipeline that gives insight in the estimated number of passengers on board of flights arriving at - and departing from [Schiphol Airport](https://www.schiphol.nl/en/ "Schiphol Airport"). It can be run both locally as well as in the cloud using *[AWS EC2](https://aws.amazon.com/ec2/ "AWS EC2")* and *[Apache Airflow](https://airflow.apache.org/ "Apache Airflow")*.
- It **extracts** `json` data from the [Schiphol API](https://www.schiphol.nl/en/developer-center/page/our-flight-api-explored/ "Schiphol API"), as well as `.csv` files with information on aircraft capacity and [passenger load factors](https://en.wikipedia.org/wiki/Passenger_load_factor "passenger load factors").
- It **transforms** the extracted data using *Pandas* into a dataframe.
- It **loads** the dataframe as a `.csv` file into *[AWS S3](https://aws.amazon.com/s3/ "AWS S3")*.

<!-- GETTING STARTED -->
## Getting Started

You can either run the project locally or in the cloud. I will give instructions for both.

### Local Installation

1. Install Python 3.10.4 and create a virtual environment.
2. Clone the repository.
   ```sh
   git clone https://github.com/has-ctrl/flights-etl-pipeline.git
   ```
3. Install the dependencies using `pip`.
   ```sh
   pip install -r requirements.txt
   ```
4. Get [Schiphol Flight API Keys](https://developer.schiphol.nl/] "Schiphol Flight API Keys") and enter keys in `.secret/api_creds.json`.

5. Create [AWS account](https://aws.amazon.com/account/ "AWS account") and enter API keys and S3 bucket_url in `.secret/aws_creds.json`.

6. Run the `test.py` script and view resulting `.csv` file in your S3 bucket.
   ```sh
   python test.py
   ```

### Cloud Installation
1. Create [AWS account](https://aws.amazon.com/account/ "AWS account"), launch an [EC2 instance](aws.amazon.com/ec2/ "EC2 instance") (Ubuntu OS with at least a 4GB RAM t3.medium instance), and save the key-pair `.pem` file in working directory.
2. Connect to your instance using an SSH client with the key-pair `.pem` file like so:
   ```sh
   ssh -i "KEY-PAIR.pem" ubuntu@ec2-X-XX-XX-XX.eu-central-1.compute.amazonaws.com
   ```
3. Install Python and `Apache Airflow` and clone the repository. 
   ```sh
   sudo apt-get update
   sudo apt install python3-pip
   sudo pip install apache-airflow
   cd airflow/
   git clone https://github.com/has-ctrl/flights-etl-pipeline.git
   ```
4. Install the dependencies using `pip`.
   ```sh
   pip install -r requirements.txt
   ```
  
5. Get [Schiphol Flight API Keys](https://developer.schiphol.nl/] "Schiphol Flight API Keys") and enter keys in `.secret/api_creds.json`.

6. Create [AWS account](https://aws.amazon.com/account/ "AWS account") and enter API keys and S3 bucket_url in `.secret/aws_creds.json`.

7. Edit the `airflow.cfg` config file by changing the dags_folder to `/home/ubuntu/airflow/flights-etl-pipeline` and setting `enable_xcom_pickling = True`.
8. Run Airflow and copy username and password from the logged output.
   ```sh
   airflow standalone
   ```
9. Optionally trigger the scheduler manually by logging into Airflow Console using the Public IPv4 DNS in your web-browser (e.g. `ec2-X-XX-XX-XX.eu-central-1.compute.amazonaws.com:8080/`) after opening port 8080 on your machine. 
