import pendulum
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'start_date': datetime(2022, 1, 23,
                           tzinfo=pendulum.timezone("Europe/Berlin")),
    'email': ['team@example.com'],
    'email_on_failure': False
}

dag = DAG(
    dag_id='test_hello_world',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['test']
)

task_hello_word = BashOperator(
    task_id='task_hello_world',
    bash_command='echo "hello world"',
    dag=dag,
)
