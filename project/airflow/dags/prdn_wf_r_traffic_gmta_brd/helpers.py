# coding=utf-8
"""Вспомогательные функции

"""
# Standard library imports
import csv
import datetime
import glob
import os
import pymssql
import shutil
import sys
import subprocess as sp
import traceback

# Third party imports
from lxml import etree
from airflow.models import Variable

# Local application imports
from prdn_wf_utils.etl_wf import etl_wf

# Клас для обработки пустых файлов
class EmptyDataException(Exception):
    pass


# Клас для обработки ошибок при отсутствии директории
class NotDirectoryException(Exception):
    pass


def delete_old_files(directory):
    """ функция очищает директорию

    Args:
        directory (str): путь к директории

    """
    for root, dirs, files in os.walk(directory, topdown=False):
        for f in files:
            curpath = os.path.join(root, f)
            os.remove(curpath)


def parse_file(file_name):
    """ функция получает из имени файла file_name, start_date, end_date and unloading_date

    Args:
        file_name (str): имя файла

    Returns:
        (dict): словарь параметров

    """
    param_dict = dict()
    parsed_value = file_name.split('_')
    param_dict['file_name'] =  file_name
    param_dict['start_date'] = str(datetime.datetime.strptime(parsed_value[-3], "%Y%m%d").date())
    param_dict['end_date'] = str(datetime.datetime.strptime(parsed_value[-2], "%Y%m%d").date())
    param_dict['unloading_date'] = datetime.datetime.strptime(parsed_value[-1].split('.')[0], "%Y%m%d%H%M%S")

    return param_dict


def copy_one_file_to_directory(file_name, directory):
    """ Функция копирует  файл в указанную директорию

    Args:
        file_name (str): имя файла
        directory (str): директория в которую копируем файл

    """

    shutil.copy(file_name, directory)


def directory_operations(work_file_name, directory, in_file_name, work_directory, f):
    """ Функция для копирования/удаления файлов из директорий

    Args:
        work_file_name (str): путь к файлу, директория work
        directory (str): директория (arch or bad)
        in_file_name (str): путь к файлу, директория in
        work_directory (str): директория work

    """
    # Перемещаем файл из work в directory
    if work_file_name:
        copy_one_file_to_directory(work_file_name, directory)

    # Удаляем файл из in и work
    delete_old_files(work_directory)
    os.remove(in_file_name)


def incorrect_completion(trace_back, failed_status, work_file_name, file_name,
                         bad_directory, in_file_name, work_directory,
                          stg_step, task_id, **kwargs):
    """ Функция для стандартных действий при обработке ошибок. Закрывает логи загрузки,
        производит операции с файлами, меняет статус в таблице регистрации файлов.

    Args:
        trace_back (str): не форматированный текст ошибки
        failed_status (str): 'FAILED'
        work_file_name (str): полный путь к файлу, директория work
        bad_directory (str): путь к директории bad
        in_file_name (str): полный путь к файлу, директория in
        work_directory (str): путь к директории work
        file_name (str): полный путь к файлу на источнике
        stg_step (Boolean): Шаг загрузки в область STG
        task_id (str): Идентификатор шага
        **kwargs (dict): Словарь с default-параметрами потока

    Returns:
        (str): функция в которой произошла ошибка

    """
    # форматируем stacktrace
    stack_trace = str(trace_back[-2]) + ' *** ' + trace_back[-1]

    # Меняем статус в таблице регистрации файлов
    etl_wf.end_file_reg_proc(failed_status, task_id, **kwargs)

    # Перемещаем/удаляем файлы из рабочих директорий
    directory_operations(work_file_name, bad_directory, in_file_name, work_directory, file_name)

    # Закрытие логов
    if stg_step:
        etl_wf.end_step_reg_proc(failed_status, task_id, stack_trace, **kwargs)

    return stack_trace


def fixml_parse(message, list_colunm):
    """ Функция парсинга одного сообщения

    Args:
        message (FiXML): сообщение о сделке в формате FiXML
        list_colunm (list): Лист с наименованием колонок

    Returns:
        list_tuple (list): Лист тьюплов для вставки в execute many

    """

    xpath_dict = {
                  'book_cd': '/BookDef/Book/@Name',
                  'desk_full_nm': '/BookDef/Book/Pty[@R="10"]/@ID',
                  'trader_name': '/BookDef/Book/@TraderName',
                  'branch': '/BookDef/Book/Pty[@R="81"]/@ID',
                  'legal_entity': '/BookDef/Book/Pty[@R="34"]/@ID',
                  'legal_entity_code': '/BookDef/Book/Pty[@R="71"]/@ID',
                  'risk_portfolio_name_ru': '/BookDef/Book/Pty[@R="78"]/@ID',
                  'source_name': '/BookDef/Book/@SrcName',
                  'created': '/BookDef/Book/@Created',
                  'modified': '/BookDef/Book/@Modified',
                  'book_modification_date': '/BookDef/Book/Pty[@R="68"]/@ID'
                 }
    sql_text = 'insert into prdn_stg.stg.book ({val}) values ({val})'
    list_tuple = list()

    fixml_substr = message.replace('xmlns="http://svn.msk.trd.ru/xsd/fixml"', 'xmlns=""')\
                          .replace('<?xml version="1.0" encoding="UTF-8"?>', '')\
                          .replace('xmlns="http://www.w3.org/2000/09/xmldsig#"', 'xmlns=""')\
                          .replace('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>', '')\
                          .replace('ns2:', '')
    root = etree.fromstring(bytes(fixml_substr, encoding='utf-8'))
    tree = etree.ElementTree(root)

    brd_list = tree.xpath('/FIXML/Batch/BookDef')

    if brd_list:
        for i, value_brd in enumerate(brd_list):
            tree = etree.ElementTree(value_brd)
            val_list = list()
            if tree.xpath(xpath_dict['desk_full_nm']) and \
                    tree.xpath(xpath_dict['source_name'])[0] in ['KONDOR', 'MUREX']:
                for el in list_colunm:
                    value = tree.xpath(xpath_dict[el])
                    if value:
                        val_list.append(value[0])
                    else:
                        val_list.append(None)
                val_tuple = tuple(val_list)
                list_tuple.append(val_tuple)
    return list_tuple


def loading_to_stg(work_file_name, task_id, **kwargs):
    """ Функция загружает данные из файла в stg

    Args:
        work_file_name (str): путь + имя файла
        task_id (str): Идентификатор шага
        **kwargs (dict): Словарь с default-параметрами потока

    """
    date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')
    file_id = kwargs['ti'].xcom_pull(task_ids=task_id, key='file_id')
    insert_date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    list_colunm = ['source_name', 'book_cd', 'desk_full_nm', 'trader_name', 'branch', 'legal_entity',
                   'legal_entity_code', 'risk_portfolio_name_ru', 'book_modification_date', 'created', 'modified']

    with open(work_file_name, 'r', encoding='utf-8') as f_obj:
        messages_list = f_obj.readlines()

    # Получаем лист листов запросов
    parse_result = list()
    for message in messages_list:
        if message:
            parse_result += fixml_parse(message, list_colunm)

    sql_truncate = "truncate table prdn_stg.stg.book"

    src_cols_str = ", ".join(list_colunm)

    sql_template = ("insert into prdn_stg.stg.book (wf_load_id, insert_date_time, {0}) values ({1}, '{2}', "
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(src_cols_str, wf_load_id, insert_date_time)
                   )

    if parse_result:
        # очищаем stg
        etl_wf.mssql_hook_execute(sql_truncate, kwargs['mssql_conn_id'])

        # загружаем stg
        etl_wf.mssql_hook_executemany(sql_template, parse_result, kwargs['mssql_conn_id'])
    else:
        raise EmptyDataException

def loading_to_gmta(**kwargs):
    """ Функция загружает данные из stg в gmta

    Args:
        **kwargs (dict): Словарь с default-параметрами потока

    """
    wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')

    sql = ("""exec prdn_gmta.rdv.sp_traffic_gmta_brd @wf_load_id = {0}""".format(wf_load_id))
    etl_wf.mssql_hook_execute(sql, kwargs['mssql_conn_id'])

    sql = ("""exec prdn_gmta.rdv.sp_fill_ref_book @wf_load_id = {0}""".format(wf_load_id))
    etl_wf.mssql_hook_execute(sql, kwargs['mssql_conn_id'])

def load_data_files_to_mssql(**kwargs):
    """ Функция загружает все файлы из директории in в mssql

    Args:
        **kwargs (dict): Словарь с default-параметрами потока

    """
    # Получаем параметры
    files_list = kwargs['ti'].xcom_pull(key='files_list')
    parameters_dict = etl_wf.get_parameters(**kwargs)
    in_directory = parameters_dict['in_directory']
    work_directory = parameters_dict['work_directory']
    arch_directory = parameters_dict['arch_directory']
    bad_directory = parameters_dict['bad_directory']
    file_extention = parameters_dict['file_extention']
    file_mask = parameters_dict['file_mask']
    success_status = 'SUCCESS'
    failed_status = 'FAILED'
    warning_status = 'WARNING'
    stg_step = False
    task_id = kwargs['ti'].task_id
    work_file_name = None
    in_file_name = None

    if not files_list:
        subtask_name = 'src: teradata; tgt: stg; obj: brokerage; descr: load_log_new_files_not_existing'
        etl_wf.start_step_reg_proc(subtask_name, **kwargs)
        etl_wf.end_step_reg_proc(warning_status, task_id, **kwargs)

    for f in files_list:
        in_file_name = os.path.normpath(os.path.join(in_directory,f))

        try:
            stg_step = True
            # Запись в лог о начале загрузки файлов
            parsed_date = str(datetime.datetime.strptime(f.split('_')[-2], "%Y%m%d").date())
            subtask_name = 'src: traffic; tgt: stg; obj: brd; date: {1}; descr: load_log_stg_{0}'.format(f, parsed_date)
            etl_wf.start_step_reg_proc(subtask_name, **kwargs)

            # Проверяем существование директорий
            dir_list =[in_directory, work_directory, arch_directory, bad_directory]
            for directory in dir_list:
                if not os.path.exists(directory) or not os.path.isdir(directory):
                    raise NotDirectoryException

            # Регистрируем файл в таблице регистрации файлов
            parsed_file_name = parse_file(f)
            etl_wf.start_file_reg_proc(parsed_file_name, file_mask, **kwargs)

            # Очишаем директорию перед копированием
            delete_old_files(work_directory)
            copy_one_file_to_directory(in_file_name, work_directory)

            # Задаем пути к директориям
            work_file_name = os.path.normpath(os.path.join(work_directory,f))

            # Загружаем данные из файла в stg
            loading_to_stg(work_file_name, task_id, **kwargs)

            # Запись в лог о завершении копирования в stg
            etl_wf.end_step_reg_proc(success_status, task_id, **kwargs)
            stg_step = False

            # Загружаем данные из stg в gmta
            loading_to_gmta(**kwargs)

            # Меняем статус в таблице регистрации файлов
            etl_wf.end_file_reg_proc(success_status, task_id, **kwargs)

            # Перемещаем/удаляем файлы из рабочих директорий
            directory_operations(work_file_name, arch_directory, in_file_name, work_directory, f)

        except NotDirectoryException:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            # извлекаем неотформатированный stack traces в виде кортежа
            trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

            # Форматирование stacktrace
            stack_trace = str(trace_back[-2]) + ' *** ' + trace_back[-1]

            # Закрываем лог с ошибкой
            etl_wf.end_step_reg_proc(failed_status, task_id, 'directory_not_existing', **kwargs)

            raise Exception(stack_trace)

        except EmptyDataException:
            # Обработка ошибки, в случае пустого файла
            # Меняем статус в таблице регистрации файлов
            etl_wf.end_file_reg_proc(warning_status, task_id, **kwargs)

            # Перемещаем/удаляем файлы из рабочих директорий
            directory_operations(work_file_name, bad_directory, in_file_name, work_directory, f)

            # Запись в лог о завершении копирования в stg
            etl_wf.end_step_reg_proc(warning_status, task_id, 'file is empty', **kwargs)

        except OSError as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            # извлеч неотформатированный stack traces в виде кортежа
            trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

            # Стандартные шаги обраотки ошибок
            stack_trace = incorrect_completion(trace_back, failed_status, work_file_name, f,
                                               bad_directory, in_file_name, work_directory,
                                               stg_step, task_id, **kwargs)
            raise Exception(stack_trace)

        except sp.CalledProcessError as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            # извлеч неотформатированный stack traces в виде кортежа
            trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

            # Стандартные шаги обраотки ошибок
            stack_trace = incorrect_completion(trace_back, failed_status, work_file_name, f,
                                               bad_directory, in_file_name, work_directory,
                                               stg_step, task_id, **kwargs)
            raise Exception(stack_trace)

        except pymssql.OperationalError as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            # извлеч неотформатированный stack traces в виде кортежа
            trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

            # Стандартные шаги обраотки ошибок
            stack_trace = incorrect_completion(trace_back, failed_status, work_file_name, f,
                                               bad_directory, in_file_name, work_directory,
                                               stg_step, task_id, **kwargs)
            raise Exception(stack_trace)

        except Exception as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            # извлеч неотформатированный stack traces в виде кортежа
            trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

            # Стандартные шаги обраотки ошибок
            stack_trace = incorrect_completion(trace_back, failed_status, work_file_name, f,
                                               bad_directory, in_file_name, work_directory,
                                               stg_step, task_id, **kwargs)
            raise Exception(stack_trace)


def read_from_traffic(**kwargs):
    """ Функция читает все сообщения из traffic и записывает в файл

    Args:
        **kwargs (dict): Словарь с default-параметрами потока

    Returns:
        (list): список имен файлов для загрузки

    """
    try:
        subtask_name = 'descr: read_from_traffic'
        success_status = 'SUCCESS'
        failed_status = 'FAILED'
        warning_status = 'WARNING'

        # Получаем параметры
        parameters_dict = etl_wf.get_parameters(**kwargs)
        in_directory = parameters_dict['in_directory']
        file_extention = parameters_dict['file_extention']
        server = parameters_dict['server']
        port = parameters_dict['port']
        queue = parameters_dict['queue']
        file_mask = parameters_dict['file_mask']
        start_date = datetime.datetime.strptime(parameters_dict['start_date'], '%Y-%m-%d').strftime("%Y%m%d")
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        unloading_date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        task_id = kwargs['ti'].task_id
        # Путь к Python 2.7
        python2_path = Variable.get('python2')

        # Запись в лог о начале чтения из traffic
        etl_wf.start_step_reg_proc(subtask_name, **kwargs)

        # Проверяем существование директорий
        dir_list =[in_directory]
        for directory in dir_list:
            if not os.path.exists(directory) or not os.path.isdir(directory):
                raise NotDirectoryException

        # Очишаем директорию перед копированием
        delete_old_files(in_directory)
        file_name = file_mask + start_date + '_' + end_date + '_' + unloading_date + file_extention

        # Определяем свежие файлы
        files_list_parsed = list()
        files_list_parsed.append(parse_file(file_name))
        files_list = sorted(etl_wf.define_new_files(files_list_parsed, file_mask, **kwargs))

        # Читаем данные из traffic
        if files_list:
            bash_command = ("{5} /data/airflow/dags/prdn_wf_utils/traffic_consumer.py "
                            "-server {0} -port {1} -queue {2} -file_dir {3} -file_name {4}"
                            .format(server, port, queue,in_directory, file_name, python2_path))

            sp.check_output(bash_command, stderr=sp.STDOUT, shell=True)
            
            # Запись в лог о завершении чтения сообщений из traffic
            etl_wf.end_step_reg_proc(success_status, task_id, **kwargs)
            kwargs['ti'].xcom_push(key='files_list', value=files_list)
        else:
            subtask_name = 'src: traffic; tgt: directiry; obj: brd; descr: no_data_in_queue'
            etl_wf.start_step_reg_proc(subtask_name, **kwargs)
            etl_wf.end_step_reg_proc(warning_status, task_id, **kwargs)

    except NotDirectoryException:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        # извлекаем неотформатированный stack traces в виде кортежа
        trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

        # Форматирование stacktrace
        stack_trace = str(trace_back[-2]) + ' *** ' + trace_back[-1]

        # Закрываем лог с ошибкой
        etl_wf.end_step_reg_proc(failed_status, task_id, 'directory_not_existing', **kwargs)

        raise Exception(stack_trace)

    except sp.CalledProcessError as e:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        # извлеч неотформатированный stack traces в виде кортежа
        trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

        # Форматирование stacktrace
        stack_trace = str(trace_back[-2]) + ' *** ' + trace_back[-1]

        # Закрываем лог с ошибкой
        etl_wf.end_step_reg_proc(failed_status, task_id, 'error_while_reading_from_traffic', **kwargs)

        raise Exception(stack_trace)

    except Exception as e:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        # извлекаем неотформатированный stack traces в виде кортежа
        trace_back = traceback.format_exception(ex_type, ex_value, ex_traceback)

        # Форматирование stacktrace
        stack_trace = str(trace_back[-2]) + ' *** ' + trace_back[-1]

        # Закрываем лог с ошибкой
        etl_wf.end_step_reg_proc(failed_status, task_id, stack_trace, **kwargs)

        raise Exception(stack_trace)
