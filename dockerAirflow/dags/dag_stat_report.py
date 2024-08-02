from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from library.emailRobot import email_robot
from library.validation_reports import get_hello
import json

loc_tzon = pytz.timezone('Asia/Dushanbe')
start_date = datetime(2024, 7, 8, 10, 0, 0 ,0, tzinfo = loc_tzon)

default_args = {
    'owner': 'FIRDAVS',
    #'depends_on_past': False,
    'start_date': start_date,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

def get_emails ():
    # for debag purpose only
    if __name__ == "__main__": dag.test() 
    
    # main code
    path = Path(__file__).parent.parent
    print (f'path of config file:  {path}')
    email_conf = json.load (open(path / ("config/email_conf.json")))
    robot = email_robot(email_conf)
    robot.fetch_emails()
    return True

def send_emails ():
    # for debag purpose only
    if __name__ == "__main__": dag.test() 
     
    # main code
    path = Path(__file__).parent.parent
    print (f'path of config file:  {path}')
    email_conf = json.load (open(path / ("config/email_conf.json")))
    robot = email_robot(email_conf)
    robot.send_validation_results()
    return True

def validate_submissions():
    get_hello()

    
  
with DAG(
    default_args=default_args,
    dag_id="reports_workflow",  
    schedule='*/5 8-21 * * 1-5',
    catchup = False, # runting dag for past periods on True
    max_active_runs = 6,
    max_active_tasks = 40
    
    
 
) as dag:
    task_get_emails = PythonOperator(
        task_id='id_task_get_emails',
        python_callable=get_emails
    )

    task_send_emails = PythonOperator(
        task_id='id_task_send_emails',
        python_callable=send_emails,
    )

    task_validate_submissions = PythonOperator(
        task_id='task_validate_submissions',
        python_callable=validate_submissions,
    )


    # task2 = PythonOperator(
    #     task_id='get_plugin',
    #     python_callable=get_plugin
    # )

    task_get_emails >> task_send_emails
    task_validate_submissions

    # >> task2