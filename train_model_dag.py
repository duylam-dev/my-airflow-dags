from airflow import DAG
# SỬA 1: Import chuẩn cho Airflow 2.10.0
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
import os

# Cảnh báo: Image mặc định của Airflow có thể chưa cài thư viện 'hdfs'
# Nếu chưa cài, task check file sẽ bị lỗi.
try:
    from hdfs import InsecureClient
except ImportError:
    print("Warning: 'hdfs' library not found. Please install 'hdfs' via pip in your Docker image.")
    InsecureClient = None

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False
}

HDFS_URL = os.getenv("HDFS_URI", "hdfs://namenode:9000")

def check_file_hdfs(path):
    if InsecureClient is None:
        raise ImportError("Library 'hdfs' is missing. Please install it in the Airflow Image.")
        
    # SỬA 2: Sửa lỗi chính tả 'cilent' -> 'client'
    client = InsecureClient(HDFS_URL, user="hdfs") 
    try:
        files = client.list(path) 
        for file in files:
            if file.endswith("parquet"):
                return True
        return False
    except Exception as e:
        print(f"Error checking HDFS: {e}")
        return False

with DAG(
    dag_id="train_model", 
    description="This is train model predict sentiment", 
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    catchup=False
) as dag:

    # SỬA 3: PythonSensor chuẩn của Airflow 2
    wait_file_review = PythonSensor(
        task_id="wait_file_review", 
        python_callable=check_file_hdfs,
        poke_interval=10,
        timeout=300,
        mode="poke",
        op_kwargs={"path": "/yelp-sentiment/data/review"} 
    )

    wait_file_business = PythonSensor(
        task_id="wait_file_business", 
        python_callable=check_file_hdfs,
        poke_interval=10,
        timeout=300,
        mode="poke",
        op_kwargs={"path": "/yelp-sentiment/data/business"} 
    )

    wait_file_user = PythonSensor(
        task_id="wait_file_user", 
        python_callable=check_file_hdfs,
        poke_interval=10,
        timeout=300,
        mode="poke",
        op_kwargs={"path": "/yelp-sentiment/data/user"} 
    )

    train_model = KubernetesPodOperator(
        task_id="train_model",
        name="train-model",
        # SỬA 4: Đổi về namespace 'bigdata' cho khớp với hệ thống của bạn
        namespace="bigdata", 
        image="minhhaitknavn/train-model:2.0.0",
        cmds=["python", "train_model.py"],
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True
    )

    email_success = EmailOperator(
        task_id="email_success", 
        to=["minhhaiit1k68@gmail.com"],
        subject="SUCCESS: Train model",
        html_content="<h3> Train model Successfully </h3>",
        trigger_rule="all_success"
    )

    email_fail = EmailOperator(
        task_id="email_fail", 
        to=["minhhaiit1k68@gmail.com"],
        subject="FAIL: Train model",
        html_content="<h3> Train model Fail - Check Airflow logs </h3>",
        trigger_rule="one_failed"
    )

    [wait_file_review, wait_file_business, wait_file_user] >> train_model >> [email_success, email_fail]
