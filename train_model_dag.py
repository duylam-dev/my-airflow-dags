from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.sensors.python import PythonSensor
from datetime import datetime , timedelta
from hdfs import InsecureClient
import os
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


default_args = {
    "owner" : "airflow" ,
    "retries" : 3 ,
    "retry_delay" : timedelta(minutes = 5) ,
    "email_on_failure" : True ,
    "email_on_retry" : False ,
    "depends_on_past" : False
}

HDFS_URL = os.getenv("HDFS_URI" , "hdfs://hdfs-namenode:9000")

def check_file_hdfs(path) :
    cilent = InsecureClient(HDFS_URL , user = "hdfs") 
    files = cilent.list(path) 
    try :
        for file in files :
            if file.endswith("parquet") :
                return True
        return False
    except Exception as e :
        return False

with DAG(
    dag_id = "train_model" , 
    description = "This is train model predict sentiment" , 
    schedule = "@daily" ,
    start_date = datetime(2024,1,1) ,
    default_args = default_args ,
    catchup = False
) as dag :
    wait_file_review = PythonSensor(
        task_id = "wait_file_review" , 
        python_callable = check_file_hdfs ,
        poke_interval = 10 ,
        timeout = 300 ,
        mode = "poke" ,
        op_kwargs = {"path" : "/yelp-sentiment/data/review"} 
    )

    wait_file_business = PythonSensor(
        task_id = "wait_file_business" , 
        python_callable = check_file_hdfs ,
        poke_interval = 10 ,
        timeout = 300 ,
        mode = "poke" ,
        op_kwargs = {"path" : "/yelp-sentiment/data/business"} 
    )

    wait_file_user = PythonSensor(
        task_id = "wait_file_user" , 
        python_callable = check_file_hdfs ,
        poke_interval = 10 ,
        timeout = 300 ,
        mode = "poke" ,
        op_kwargs = {"path" : "/yelp-sentiment/data/user"} 
    )

    train_model = KubernetesPodOperator(
        task_id="train_model",
        name="train-model",
        namespace="airflow",  # set lại namespace
        image="minhhaitknavn/train-model:2.0.0",
        cmds=["python", "train_model.py"],
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True
    )

    email_success = EmailOperator(
        task_id = "email_success" , 
        to = ["minhhaiit1k68@gmail.com"] ,
        subject = "SUCCESS : Train model " ,
        html_content = "<h3> Train model Successfully </h3>" ,
        trigger_rule = "all_success"
    )

    email_fail = EmailOperator(
        task_id = "email_fail" , 
        to = ["minhhaiit1k68@gmail.com"] ,
        subject = "FAIL : Train model " ,
        html_content = "<h3> Train model Fail - Check Airflow logs </h3>" ,
        trigger_rule = "one_failed"
    )

    [wait_file_review ,wait_file_business , wait_file_user] >> train_model >> [email_success , email_fail]
