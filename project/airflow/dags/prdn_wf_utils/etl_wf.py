# coding=utf-8
"""Вспомогательные функции для потоков загрузки

"""
# Standard library imports
import logging

# Third party imports
from airflow.hooks.mssql_hook import MsSqlHook

# Local application imports

class etl_wf(object):
    """Класс для работы с метаданными потока в базе MSSQL GMTA.
    """

    def mssql_hook_fetchone(sql, mssql_conn_id):
        """Функция для выполнения sql запросов и получения результатов из метаданных MSSQL GMTA.
        На выход возвращается одна строка из результата запроса.

        Args:
            sql (str): Текст запроса отправляемый на выполнение
            mssql_conn_id (str): Имя подключения к базе

        Returns:
            (row): Результат запроса - одна строка

        """
        mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        conn = mssql_hook.get_conn()
        mssql_hook.set_autocommit(conn, True)
        cursor = conn.cursor()
        cursor.execute(sql)
        query_result = cursor.fetchone()

        return query_result

    def mssql_hook_fetchall(sql, mssql_conn_id):
        """Функция для выполнения sql запросов и получения результатов из метаданных MSSQL GMTA.
        На выход возвращаются все строки из результата запроса.

        Args:
            sql (str): Текст запроса отправляемый на выполнение
            mssql_conn_id (str): Имя подключения к базе

        Returns:
            (list): Результат запроса - все строки

        """
        mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        conn = mssql_hook.get_conn()
        mssql_hook.set_autocommit(conn, True)
        cursor = conn.cursor()
        cursor.execute(sql)
        query_result = cursor.fetchall()

        return query_result

    def mssql_hook_execute(sql, mssql_conn_id):
        """Функция для выполнения sql запроса без получения результатов.
        Используется для выполнения различных процедур и функций.

        Args:
            sql (str): Текст запроса отправляемый на выполнение
            mssql_conn_id (str): Имя подключения к базе

        """
        mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        conn = mssql_hook.get_conn()
        mssql_hook.set_autocommit(conn, True)
        cursor = conn.cursor()
        cursor.execute(sql)

    def mssql_hook_executemany(sql_template, sql_data, mssql_conn_id):
        """Функция для выполнения множества sql - запросов без получения результатов.
        Используется для выполнения различных вставок данных.

        Args:
            sql_template (str): Шаблон запроса на выполнение
            sql_data (list): Пачка данных отправляемых на выполнение
            mssql_conn_id (str): Имя подключения к базе

        """
        mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        conn = mssql_hook.get_conn()
        mssql_hook.set_autocommit(conn, True)
        cursor = conn.cursor()
        cursor.executemany(sql_template, sql_data)


    def workflow_registration(**kwargs):
        """Функция для регистрации потока загрузки в метаданных MSSQL GMTA.
        Выполняется процедура регистрации, которая возвращает wf_load_id,
        который в дальнейшем передаётся в остальные функции и шаги потока.

        Args:
            **kwargs (dict): Словарь с default-параметрами потока

        """
        etl_sp_start_wf = (
            "declare "
            "    @p_wf_name nvarchar(100) "
            "  , @p_wf_load_id numeric(22,0) "
            "  set @p_wf_name = '{0}' "
            "exec prdn_gmta.tmd.etl_sp_start_wf @wf_name = @p_wf_name, "
            "  @wf_load_id_new = @p_wf_load_id output "
            "select @p_wf_load_id as 'wf_load_id' "
            .format(kwargs['wf_name'])
        )

        wf_load_id = etl_wf.mssql_hook_fetchone(etl_sp_start_wf, kwargs['mssql_conn_id'])[0]
        kwargs['ti'].xcom_push(key='wf_load_id', value=wf_load_id)

    def check_workflow_registration(**kwargs):
        """Функция для проверки регистрации потока в метаданных MSSQL GMTA.
        Если наш поток не смог зарегистрироваться по какой-либо причине,
        то загрузку данных не начинаем - переходим к
        шагу потока workflow_registration_failed

        Args:
            **kwargs (dict): Словарь с default-параметрами потока

        Returns:
            (str): Имя следующего шага потока на выполнение

        """
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')

        if wf_load_id is not None:
            return 'workflow_parameters_init'
        else:
            return 'workflow_registration_failed'

    def workflow_parameters_init(**kwargs):
        """Функция динамической генерации параметров потока из метаданных MSSQL GMTA

        Args:
            **kwargs (dict): Словарь с default-параметрами потока

        """
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')

        etl_sp_wf_param_init = (
            "exec prdn_gmta.tmd.etl_sp_wf_param_init @wf_load_id = {0}"
            .format(wf_load_id)
        )

        etl_wf.mssql_hook_execute(etl_sp_wf_param_init, kwargs['mssql_conn_id'])

    def get_parameters(**kwargs):
        """Функция подтягивает параметры из базы данных для экземпляра потока

        Args:
            **kwargs (dict): Словарь с default-параметрами потока

        Returns:
            (dict): Список параметров

        """
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')
        parameters_dict = dict()

        etl_wf_param_log = (
            "select "
            "    param_name "
            "  , param_value "
            "  from prdn_gmta.tmd.etl_wf_param_log "
            " where wf_load_id = {0}"
            .format(wf_load_id)
        )

        result = etl_wf.mssql_hook_fetchall(etl_wf_param_log, kwargs['mssql_conn_id'])

        for row in result:
            parameters_dict.update({row[0]: row[1]})

        return parameters_dict

    def start_step_reg_proc(subtask_name, **kwargs):
        """Функция регистрирует в метаданных MSSQL GMTA начало шага

        Args:
            subtask_name (str): детальное описание шага
            **kwargs (dict): Словарь с default-параметрами потока

        """
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')

        etl_sp_start_wf_load_log = (
            "declare "
            "    @p_wf_load_log_id numeric(22,0) "
            "exec prdn_gmta.tmd.etl_sp_start_wf_load_log @wf_load_id = {0}, @subtask_name = '{1}', "
            "  @wf_load_log_id_new = @p_wf_load_log_id output "
            "select @p_wf_load_log_id as 'wf_load_log_id' "
            .format(wf_load_id, subtask_name)
        )

        wf_load_log_id = etl_wf.mssql_hook_fetchone(etl_sp_start_wf_load_log, kwargs['mssql_conn_id'])[0]
        kwargs['ti'].xcom_push(key='wf_load_log_id', value=wf_load_log_id)

    def end_step_reg_proc(step_status, task_id, description=None, **kwargs):
        """Функция регистрирует в метаданных MSSQL GMTA завершение шага

        Args:
            step_status (str): (SUCCESS|FAILED|WARNING)
            task_id (str): Идентификатор шага
            description (str): Дополнительное описание шага
            **kwargs (dict): Словарь с default-параметрами потока

        """
        wf_load_log_id = kwargs['ti'].xcom_pull(task_ids=task_id, key='wf_load_log_id')

        if description:
            description = "'{0}'".format(description.replace("'","''"))
        else:
            description = 'null'

        etl_sp_close_wf_load_log = (
            "exec prdn_gmta.tmd.etl_sp_close_wf_load_log @wf_load_log_id = {0}, @status = {1}, @description = {2}"
            .format(wf_load_log_id, step_status, description)
        )

        etl_wf.mssql_hook_execute(etl_sp_close_wf_load_log, kwargs['mssql_conn_id'])

    def check_workflow_final_status(step_status, next_step, subtask_name_filter=None, **kwargs):
        """Функция проверяет статусы шагов потока в метаданных MSSQL GMTA
        Если количество искомых статустов step_status больше 0, возвращаем next_step,
        иначе возвращаем успешное завершение потока workflow_closing_success

        Args:
            step_status (str): Статусы шагов для поиска в метаданных
            next_step (str): Имя следующего шага потока на выполнение
            subtask_name_filter (str): Фильтр на поле subtask_name
            **kwargs (dict): Словарь с default-параметрами потока

        Returns:
            (str): Имя следующего шага потока на выполнение

        """
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')
        next_step_list = ['workflow_closing_success', 'workflow_closing_warning', 'workflow_closing_failed']

        if next_step not in next_step_list:
            raise Exception('Incorrect next_step parameter. Possible options: {0}'.format(next_step_list))

        status_filter = "'" + "','".join([item.strip() for item in step_status.split(',')]) + "'"

        if subtask_name_filter:
            subtask_name_filter = "and subtask_name like '{0}'".format(subtask_name_filter)
        else:
            subtask_name_filter = ''

        etl_wf_load_log = (
            "declare "
            "    @count_failures int "
            "select "
            "    @count_failures = count(status) "
            "  from prdn_gmta.tmd.etl_wf_load_log "
            " where wf_load_id = {0} "
            "  and  status in ({1}) "
            "  {2} "
            "select @count_failures as 'count_failures'"
            .format(wf_load_id, status_filter, subtask_name_filter)
        )

        count_failures = etl_wf.mssql_hook_fetchone(etl_wf_load_log, kwargs['mssql_conn_id'])[0]

        if count_failures > 0:
            return next_step
        else:
            return 'workflow_closing_success'

    def workflow_closing(workflow_status, description=None, **kwargs):
        """Функция закрытия экземпляра потока в метаданных MSSQL GMTA.
        При закрытии устанавливается соответствующий итоговый статус потока.

        Args:
            workflow_status (str): Итоговый статус экземпляра потока
            description (str): Дополнительное описание
            **kwargs (dict): Словарь с default-параметрами потока

        """
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')

        if description:
            description = "'{0}'".format(description.replace("'","''"))
        else:
            description = 'null'

        etl_sp_close_wf = (
            "exec prdn_gmta.tmd.etl_sp_close_wf @wf_load_id = {0}, @status = {1}, @description = {2}"
            .format(wf_load_id, workflow_status, description)
        )

        etl_wf.mssql_hook_execute(etl_sp_close_wf, kwargs['mssql_conn_id'])

    def start_file_reg_proc(parsed_file_name, mask_name, **kwargs):
        """Функция регистрирует загружаемый файл в метаданных MSSQL GMTA

        Args:
            parsed_file_name (dict): словарь с параметрами из имени загружаемого файла
            mask_name (str): Имя маски искомого файла-выгрузки
            **kwargs (dict): Словарь с default-параметрами потока

        """
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')

        # регистрируем загружаемый файл
        etl_sp_start_wf_load_file = (
            "declare "
            "    @p_file_id numeric(22,0) "
            "exec prdn_gmta.tmd.etl_sp_start_wf_load_file @wf_load_id = {0}, @file_name = '{1}', @start_date = '{2}', "
            "@end_date = '{3}', @unloading_date = '{4}', @mask_name = '{5}', "
            "  @file_id_new = @p_file_id output "
            "select @p_file_id as 'file_id' "
            .format(wf_load_id, parsed_file_name['file_name'], parsed_file_name['start_date'],
                    parsed_file_name['end_date'], parsed_file_name['unloading_date'], mask_name)
        )

        file_id = etl_wf.mssql_hook_fetchone(etl_sp_start_wf_load_file, kwargs['mssql_conn_id'])[0]
        kwargs['ti'].xcom_push(key='file_id', value=file_id)

    def end_file_reg_proc(file_status, task_id, **kwargs):
        """Функция фиксирует финальный статус загружаемого файла в метаданных MSSQL GMTA

        Args:
            file_status (str): финальный статус загружаемого файла
            task_id (str): Идентификатор шага
            **kwargs (dict): Словарь с default-параметрами потока

        """
        file_id = kwargs['ti'].xcom_pull(task_ids=task_id, key='file_id')

        etl_sp_close_wf_load_file = (
            """exec prdn_gmta.tmd.etl_sp_close_wf_load_file @file_id = {0}, @status = '{1}'"""
            .format(file_id, file_status)
        )

        etl_wf.mssql_hook_execute(etl_sp_close_wf_load_file, kwargs['mssql_conn_id'])

    def define_new_files(files_list_parsed, mask_name, **kwargs):
        """ функция определяет имена еще не загруженных файлов на основе метаданных MSSQL GMTA

        Args:
            files_list_parsed (list): Список словарей с параметрами из имён по всем доступным файлам-выгрузок
            mask_name (str): Имя маски искомого файла-выгрузки
            **kwargs (dict): Словарь с default-параметрами потока

        Returns:
            (list): Список имён еще не загруженных файлов

        """
        new_file_list = list()
        wf_load_id = kwargs['ti'].xcom_pull(key='wf_load_id')

        for file_name_parsed in files_list_parsed:

            etl_sp_check_wf_load_file = (
                "declare "
                "    @p_result int "
                "  , @p_max_file_datetime datetime "
                "exec prdn_gmta.tmd.etl_sp_check_wf_load_file @wf_load_id = {0}, @start_date = '{1}', @end_date = '{2}', @mask_name = '{3}',"
                "  @max_file_datetime = @p_max_file_datetime output "
                "select @p_max_file_datetime as 'max_file_datetime' "
                .format(wf_load_id, file_name_parsed['start_date'], file_name_parsed['end_date'], mask_name)
            )

            max_file_datetime = etl_wf.mssql_hook_fetchone(etl_sp_check_wf_load_file, kwargs['mssql_conn_id'])[0]

            if not max_file_datetime:
                new_file_list.append(file_name_parsed['file_name'])
            elif max_file_datetime and file_name_parsed['unloading_date'] > max_file_datetime:
                new_file_list.append(file_name_parsed['file_name'])

        return new_file_list
