use prdn_gmta
go
/*
    =================================================================================================
    Author:        Костыря Владимир
    Created date:  16.11.2019
    Description:   Процедура загружает данный из rdv.ref_book в rdv.book_desk (prdn_gmta.rdv.sp_fill_ref_book)
    =================================================================================================
*/
create or alter procedure rdv.sp_fill_ref_book
    @wf_load_id numeric
as
begin try

declare
    @p_wf_load_log_id numeric --ИД лога загрузки
  , @p_file_name varchar (255) --Имя файла загрузки
  , @p_parsed_date date --максимальная дата загружаемых данных
  , @p_res_descr varchar(255)

    declare
        @p_status varchar(50)
      , @p_wf_load_id numeric(22,0)

    select
        @p_wf_load_id = wf_load_id
      , @p_status = status
      from tmd.etl_wf_load
     where wf_load_id = @wf_load_id

    --проверка наличия потока
    if @p_wf_load_id is NULL
      raiserror (N'Workflow does not exists, wf_load_id = %I64d', 11, 1, @wf_load_id)

    --проверка верного нового статуса потока
    if @p_status not IN ('START')
      raiserror (N'Status = %s, but must be = "START" ', 11, 2, @p_status)

    --лог. регистрация шага загрузки.
    select
        @p_file_name = max(file_name)
      from prdn_gmta.tmd.etl_wf_load_file
     where wf_load_id = @wf_load_id
       and status = 'START'

    --Получаем максимальную дату загружаемых данных из имени файла
    select
        @p_parsed_date = convert(date, max(value), 23)
      from string_split(@p_file_name, '_')
     where value like '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
    set @p_file_name = 'src: gmta; tgt: gmta; obj: brd; ' + convert(varchar, @p_parsed_date, 23) + '; descr: load_log_gmta_' + @p_file_name

        exec prdn_gmta.tmd.etl_sp_start_wf_load_log
            @wf_load_id = @wf_load_id
          , @subtask_name = @p_file_name
          , @wf_load_log_id_new = @p_wf_load_log_id output

begin transaction;

declare
    @old_cnt int
  , @new_cnt int
  , @empty_date datetime --Дата '5999-12-31 23:59:59'
  , @insert_date datetime --Дата вставки
set @empty_date = '5999-12-31 23:59:59'
set @insert_date = getdate()

--Получаем количесво строк в таблице
select @old_cnt = count(*) from prdn_gmta.rdv.v_ref_book_desk

--Получаем количесво строк готовых к вставке
select
      @new_cnt = count(*)
      from prdn_gmta.rdv.ref_book as t1
        inner join prdn_gmta.rdv.ref_desk as t2
          on t1.desk_full_nm = t2.desk_full_nm
     where t1.eff_to_dttm = @empty_date

--Если количество, вставляемых, строк >= количеству текущих, то обновляем данные.
if @new_cnt>=@old_cnt
    begin
        delete from prdn_gmta.rdv.ref_book_desk;

        insert into prdn_gmta.rdv.ref_book_desk (source_name, book_cd, load_dttm, src_cd, desk_cd)
            select
                t1.source_name
              , t1.book_cd
              , @insert_date
              , t1.src_cd
              , t2.desk_cd
              from prdn_gmta.rdv.ref_book as t1
                inner join prdn_gmta.rdv.ref_desk as t2
                  on t1.desk_full_nm = t2.desk_full_nm
             where t1.eff_to_dttm = @empty_date;

        --лог. успешное завершение шага загрузки.
        exec prdn_gmta.tmd.etl_sp_close_wf_load_log
            @wf_load_log_id = @p_wf_load_log_id
          , @status = 'SUCCESS'
          , @description = null
    end
else
    --лог. успешное завершение шага загрузки.
    exec prdn_gmta.tmd.etl_sp_close_wf_load_log
        @wf_load_log_id = @p_wf_load_log_id
      , @status = 'WARNING'
      , @description = 'The count of rows is less than expected. Insert canceled'

commit transaction;

end try

begin catch
    declare @ErrorMessage nvarchar(4000);
    declare @ErrorSeverity int;
    declare @ErrorState int;

    select
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE();

    set @p_res_descr = 'ERROR_NUMBER: ' + cast(error_number() as varchar(64)) + ';  ERROR_MESSAGE: ' + error_message();

    --лог. ошибка загрузки в таблицу
    if exists (select 1 from prdn_gmta.tmd.etl_wf_load_log where wf_load_log_id = @p_wf_load_log_id)
        exec prdn_gmta.tmd.etl_sp_close_wf_load_log
            @wf_load_log_id = @p_wf_load_log_id
          , @status = 'FAILED'
          , @description = @p_res_descr

    raiserror (@ErrorMessage, -- Message text.
               @ErrorSeverity, -- Severity.
               @ErrorState -- State.
               );

    set nocount off
    return 1

end catch

go

grant execute on rdv.sp_fill_ref_book to prdn_r_gmta_tech_loader;
go
