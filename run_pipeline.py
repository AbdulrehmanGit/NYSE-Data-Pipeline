import subprocess
from dotenv import load_dotenv
import os
import logging

# Setup logging
logging.basicConfig(
    filename="transfer_to_hdfs.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Load environment variables
load_dotenv()
ssh_key = os.getenv('ssh_key')
remote_user = os.getenv('remote_user')
remote_host = os.getenv('remote_host')

# Define all file and HDFS paths
datasets = [
    {
        "name": "fact_daily_price",
        "local_path": "D:/Workspace/NYSE/data/prepared/fact_daily_price/*.parquet",
        "remote_path": "/home/cloudera/NYSE/fact/",
        "hdfs_path": "/user/cloudera/nyse/facts/"
    },
    {
        "name": "dim_ticker",
        "local_path": "D:/Workspace/NYSE/data/prepared/dim_ticker/*.parquet",
        "remote_path": "/home/cloudera/NYSE/ticker/",
        "hdfs_path": "/user/cloudera/nyse/dim_ticker/"
    },
    {
        "name": "dim_date",
        "local_path": "D:/Workspace/NYSE/data/prepared/dim_date/*.parquet",
        "remote_path": "/home/cloudera/NYSE/date/",
        "hdfs_path": "/user/cloudera/nyse/dim_date/"
    }
]

for data in datasets:
    try:
        logging.info(f"Starting transfer for {data['name']}")

        # Step 1: Copy from local to Cloudera VM via SCP
        scp_cmd = f'scp -i {ssh_key} -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedKeyTypes=+ssh-rsa "{data["local_path"]}" {remote_user}@{remote_host}:{data["remote_path"]}'
        logging.info(f"Running SCP command: {scp_cmd}")
        subprocess.run(scp_cmd, shell=True, check=True)
        logging.info(f"SCP transfer completed for {data['name']}")

        # Step 2: Move files from Cloudera local to HDFS
        hdfs_cmd = f'ssh -i {ssh_key} -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedKeyTypes=+ssh-rsa {remote_user}@{remote_host} "hdfs dfs -mkdir -p {data["hdfs_path"]} && hdfs dfs -put -f {data["remote_path"]}*.parquet {data["hdfs_path"]}"'
        logging.info(f"Running HDFS command: {hdfs_cmd}")
        subprocess.run(hdfs_cmd, shell=True, check=True)
        logging.info(f"HDFS transfer completed for {data['name']}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error while processing {data['name']}: {e}")
    except Exception as ex:
        logging.exception(f"Unexpected error for {data['name']}: {ex}")
