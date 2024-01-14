# Licensed ...
"""
### DAG Documentation
Markdown style
"""
from __future__ import annotations

# [START spark_dag]
# [START import_module]
import json
import textwrap

import pendulum

from datetime import datetime

from support.router import run

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# [END import_module]

def interpreter(cmd_name, *args):
    with open("airlogs_spark.log", "a", encoding="utf-8") as f:
        params = ' '.join(args)
        f.write(f"{datetime.now()} {cmd_name} call, params: {params}\n")
    run("parser.py", cmd_name, *args)

def spark_run():
    js = json.load(open('/home/irina/spark_dag.json', encoding="utf-8"))
    interpreter("spark", js['date1'], js['date2'], js['terms'])

# [START init_dag]
with DAG(
    "spark_dag",
    # [START default_args]
    default_args={"retries": 1},
    # [END default_args]
    description="Spark DAG",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["my_dag"],
) as dag:
    # [END init_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START main_flow]
    spark_task = PythonOperator(
        task_id="spark_task_id",
        python_callable=spark_run,
    )
    spark_task.doc_md = textwrap.dedent(
        """\
    #### Spark task
    Description
    """
    )

    spark_task

# [END main_flow]
# [END spark_dag]
