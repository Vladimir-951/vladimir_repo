# coding=utf-8
"""Поток загрузки brd из источника Teradata в базу MSSQL (GMTA).
Загрузка проходит на регулярной основе - ежедневно.
"""

# Standard library imports
from datetime import datetime, timedelta
import pendulum

# Third party imports
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Local application imports
from prdn_wf_utils.etl_wf import etl_wf
import prdn_wf_r_traffic_gmta_brd.helpers as helpers

# different default arguments for DAG
local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'gmta',
    'depends_on_past': False,
    'start_date': datetime(year=2019, month=4, day=5, tzinfo=local_tz),
    'email': ['123@mail.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'provide_context': True,
    'mssql_conn_id': 'prdn_gmta_mssql_main',
    'wf_name': 'prdn_wf_r_traffic_gmta_brd'
}

with DAG (dag_id='prdn_wf_r_traffic_gmta_brd', default_args=default_args, catchup=False, schedule_interval='0 05 * * *') as dag:

    # workflow_registration task definition - регистрация потока в метаданных mssql
    workflow_registration = PythonOperator(
        task_id='workflow_registration',
        python_callable=etl_wf.workflow_registration,
        op_kwargs=default_args
    )

    # check_workflow_registration task definition - проверка регистрации потока в метаданных mssql
    check_workflow_registration = BranchPythonOperator(
        task_id='check_workflow_registration',
        python_callable=etl_wf.check_workflow_registration,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # workflow_registration_failed task definition - поток не смог зарегистрироваться в матаданных, прерываем загрузку
    workflow_registration_failed = DummyOperator(
        task_id='workflow_registration_failed'
    )

    # workflow_parameters_init task definition - генерация параметров для потока из метаданных mssql
    workflow_parameters_init = PythonOperator(
        task_id='workflow_parameters_init',
        python_callable=etl_wf.workflow_parameters_init,
        op_kwargs=default_args
    )

    # copy_from_remout_source definition - копироварие файлов из traffic в директорию "in"
    copy_from_remout_source = PythonOperator(
        task_id='read_from_traffic',
        python_callable=helpers.read_from_traffic,
        op_kwargs=default_args
    )

    # load_to_stage definition - Загрузка файлов в mssql
    load_to_mssql = PythonOperator(
        task_id='load_to_mssql',
        python_callable=helpers.load_data_files_to_mssql,
        op_kwargs=default_args
    )

    # check_workflow_final_status definition - с каким статусом завершаем поток SUCCESS or WARNING mssql
    check_workflow_final_status = BranchPythonOperator(
        task_id='check_workflow_final_status',
        python_callable=etl_wf.check_workflow_final_status,
        op_args=['WARNING, FAILED', 'workflow_closing_warning', '%descr: load_log_%'],
        op_kwargs=default_args
    )

    # workflow_closing_warning definition - завершение потока с замечаниями. закрываем со статусом warning в метаданных mssql
    workflow_closing_warning = PythonOperator(
        task_id='workflow_closing_warning',
        python_callable=etl_wf.workflow_closing,
        op_args=['WARNING'],
        op_kwargs=default_args
    )

    # workflow_closing_success definition - успешное завершение потока, закрываем со статусом success в метаданных mssql
    workflow_closing_success = PythonOperator(
        task_id='workflow_closing_success',
        python_callable=etl_wf.workflow_closing,
        op_args=['SUCCESS'],
        op_kwargs=default_args
    )

    # workflow_closing_failed definition - не успешное завершение потока, закрываем со статусом failed в метаданных mssql
    workflow_closing_failed = PythonOperator(
        task_id='workflow_closing_failed',
        python_callable=etl_wf.workflow_closing,
        op_args=['FAILED'],
        op_kwargs=default_args,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # tasks dependencies definition - зависимости между различными задачами потока
    workflow_registration >> check_workflow_registration >> [workflow_parameters_init, workflow_registration_failed]
    workflow_registration >> workflow_registration_failed
    workflow_parameters_init >> copy_from_remout_source >> load_to_mssql >> check_workflow_final_status >> [workflow_closing_success, workflow_closing_warning]
    [workflow_parameters_init, load_to_mssql, copy_from_remout_source, check_workflow_final_status] >> workflow_closing_failed
