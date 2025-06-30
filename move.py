import os
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv

# Determine repository root and load environment
repo_root = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=repo_root / '.env')

# Setup logging
log_dir = repo_root / os.getenv('LOG_DIR', 'logs')
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'transfer_to_hdfs.log'
logging.basicConfig(
    filename=str(log_file),
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment variables
ssh_key = os.getenv('SSH_KEY')
remote_user = os.getenv('REMOTE_USER')
remote_host = os.getenv('REMOTE_HOST')

# Define datasets with local glob paths and HDFS targets
datasets = [
    {
        'name': 'fact_daily_price',
        'local_glob': str(repo_root / 'data' / 'prepared' / 'fact_daily_price' / '*.parquet'),
        'remote_path': '/home/cloudera/NYSE/fact/',
        'hdfs_path': '/user/cloudera/nyse/facts/'
    },
    {
        'name': 'dim_ticker',
        'local_glob': str(repo_root / 'data' / 'prepared' / 'dim_ticker' / '*.parquet'),
        'remote_path': '/home/cloudera/NYSE/ticker/',
        'hdfs_path': '/user/cloudera/nyse/dim_ticker/'
    },
    {
        'name': 'dim_date',
        'local_glob': str(repo_root / 'data' / 'prepared' / 'dim_date' / '*.parquet'),
        'remote_path': '/home/cloudera/NYSE/date/',
        'hdfs_path': '/user/cloudera/nyse/dim_date/'
    }
]

for data in datasets:
    try:
        logger.info(f"Starting transfer for {data['name']}")

        # SCP: local to VM
        scp_cmd = [
            'scp', '-i', ssh_key,
            '-o', 'HostKeyAlgorithms=+ssh-rsa',
            '-o', 'PubkeyAcceptedKeyTypes=+ssh-rsa',
            data['local_glob'],
            f"{remote_user}@{remote_host}:{data['remote_path']}"
        ]
        logger.info(f"Running SCP: {' '.join(scp_cmd)}")
        subprocess.run(scp_cmd, check=True)
        logger.info(f"SCP completed for {data['name']}")

        # HDFS put via SSH
        hdfs_cmd = (
            f"ssh -i {ssh_key} -o HostKeyAlgorithms=+ssh-rsa "
            f"-o PubkeyAcceptedKeyTypes=+ssh-rsa {remote_user}@{remote_host} "
            f"\"hdfs dfs -mkdir -p {data['hdfs_path']} && hdfs dfs -put -f {data['remote_path']}*.parquet {data['hdfs_path']}\""
        )
        logger.info(f"Running HDFS upload: {hdfs_cmd}")
        subprocess.run(hdfs_cmd, shell=True, check=True)
        logger.info(f"HDFS transfer completed for {data['name']}\n")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error in SCP/HDFS for {data['name']}: {e}")
    except Exception as ex:
        logger.exception(f"Unexpected error for {data['name']}: {ex}")
