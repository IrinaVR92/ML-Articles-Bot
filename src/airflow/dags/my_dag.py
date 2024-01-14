# Licensed ...
"""
### DAG Documentation
Markdown style
"""
from __future__ import annotations

# [START my_dag]
# [START import_module]
import textwrap

import pendulum

from datetime import datetime, timedelta

from support.router import run

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# [END import_module]

def interpreter(cmd_name, *args):
    with open("airlogs.log", "a", encoding="utf-8") as f:
        params = ' '.join(args)
        f.write(f"{datetime.now()} {cmd_name} call, params: {params}\n")
    run("parser.py", cmd_name, *args)

def arxiv_run(date1, date2):
    interpreter("arxiv2csv", date1, date2)

def cwp_run():
    interpreter("cwp2csv")

def csv2db_run(date1, date2):
    interpreter("csv2db", date1, date2)

def purge_selenium():
    interpreter("purge_selenium")

time_now = datetime.now()
time_dbeg = timedelta(days=-7)
time_dend = timedelta(days=0)
time_transf = lambda n, d: str(n + d).split(" ")[0]
date1, date2 = time_transf(time_now, time_dbeg), time_transf(time_now, time_dend)

# [START init_dag]
with DAG(
    "my_dag",
    # [START default_args]
    default_args={"retries": 1},
    # [END default_args]
    description="My First DAG",
    schedule="0 0 * * 1",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["my_dag"],
) as dag:
    # [END init_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START main_flow]
    arxiv_task = PythonOperator(
        task_id="arxiv",
        python_callable=arxiv_run,
        op_kwargs = {
            'date1': date1,
            'date2': date2,
        }
    )
    arxiv_task.doc_md = textwrap.dedent(
        """\
    #### Arxiv task
    Description
    """
    )

    cwp_task = PythonOperator(
        task_id="cwp",
        python_callable=cwp_run,
    )
    cwp_task.doc_md = textwrap.dedent(
        """\
    #### CWP task
    Description
    """
    )
    
    csv2db_task = PythonOperator(
        task_id="csv2db",
        python_callable=csv2db_run,
        op_kwargs = {
            'date1': date1,
            'date2': date2,
        }
    )
    csv2db_task.doc_md = textwrap.dedent(
        """\
    #### csv2db task
    Description
    """
    )
    
    purge_selenium_task = PythonOperator(
        task_id="purge_selenium",
        python_callable=purge_selenium,
    )
    purge_selenium_task.doc_md = textwrap.dedent(
        """\
    #### Purge Selenium task
    Description
    """
    )

    [arxiv_task, cwp_task] >> csv2db_task >> purge_selenium_task

# [END main_flow]
# [END my_dag]
