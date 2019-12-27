use prdn_gmta
go
/*
    =================================================================================================
    Author:        Костыря Владимир
    Created date:  10.11.2019
    Description:   Процедура загружает данный из stg в gmta (prdn_gmta.rdv.sp_traffic_gmta_brd)
    =================================================================================================
*/
create or alter procedure rdv.sp_traffic_gmta_brd
    @wf_load_id numeric
as
begin
    set nocount on;
    set transaction isolation level read uncommitted;
begin try

declare
    @p_wf_load_log_id numeric --ИД лога загрузки
  , @p_transact_name varchar (70) --наименование транзакции
  , @p_file_name varchar (255) --Имя файла загрузки
  , @p_parsed_date date --максимальная дата загружаемых данных
  , @p_res_descr varchar(255) --Описание ошибки
  , @curr_date datetime --Текущее датавремя
  , @empty_date datetime --Дата '5999-12-31 23:59:59'
  , @src_cd varchar(10)--Тип источника
  , @p_sql nvarchar(max)--Текст запроса
    set @curr_date = getdate();
    set @empty_date = '5999-12-31 23:59:59'

    declare
        @p_status varchar(50)
      , @p_wf_load_id numeric(22,0)

    select
        @p_wf_load_id = wf_load_id
      , @p_status = status
      from tmd.etl_wf_load
     where wf_load_id = @wf_load_id

    --проверка наличия потока
    if @p_wf_load_id is null
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
    set @p_file_name = 'src: stg; tgt: gmta; obj: brd; ' + convert(varchar, @p_parsed_date, 23) + '; descr: load_warning_desk_log_gmta_' + @p_file_name

    --Получаем название источника
    select
        @src_cd = src_name
      from prdn_gmta.tmd.etl_src
     where src_id = (select 
                        src_id
                      from prdn_gmta.tmd.etl_wf
                     where wf_id =  (select
                                        wf_id
                                      from prdn_gmta.tmd.etl_wf_load
                                     where wf_load_id = @wf_load_id))

    exec prdn_gmta.tmd.etl_sp_start_wf_load_log
        @wf_load_id = @wf_load_id
      , @subtask_name = @p_file_name
      , @wf_load_log_id_new = @p_wf_load_log_id output

    --удаляем временные таблицы, если есть
    if object_id('tempDB..##t_max_date') is not null
      drop table ##t_max_date
    if object_id('tempDB..##stg_brd') is not null
      drop table ##stg_brd
    if object_id('tempDB..##stg_diff') is not null
      drop table ##stg_diff
    if object_id('tempDB..##t_max_date_grouped') is not null
      drop table ##t_max_date_grouped

begin transaction

--Временная таблица для определения актуальных записей и избавления от дублей
    create table ##t_max_date_grouped (
        book_cd_max varchar(30) collate Cyrillic_General_CI_AS not null
      , source_name_max varchar(10) collate Cyrillic_General_CI_AS not null
      , max_date datetime)

    set @p_sql = 'insert into ##t_max_date_grouped
                      select 
                          book_cd
                        , source_name
                        , max(book_modification_date)
                        from prdn_stg.stg.book
                       where wf_load_id = ''' + cast(@wf_load_id as nvarchar) + '''
                      group by book_cd, source_name'

    exec dbo.sp_executesql @p_sql

    --Временная таблица для определения актуальных записей и избавления от дублей
    create table ##t_max_date (
        wf_load_id numeric(22, 0) not null
      , desk_full_nm varchar(50) collate Cyrillic_General_CI_AS not null
      , source_name varchar(10) collate Cyrillic_General_CI_AS not null
      , book_cd varchar(30) collate Cyrillic_General_CI_AS not null
      , trader_name varchar(50) collate Cyrillic_General_CI_AS null
      , branch varchar(10) collate Cyrillic_General_CI_AS null
      , legal_entity varchar(50) collate Cyrillic_General_CI_AS null
      , legal_entity_code varchar(20) collate Cyrillic_General_CI_AS null
      , risk_portfolio_name_ru varchar(300) collate Cyrillic_General_CI_AS null
      , book_modification_date datetime not null
      , created datetime null
      , modified datetime null)

    set @p_sql = 'insert into ##t_max_date
                      select 
                          bk.wf_load_id
                        , bk.desk_full_nm
                        , bk.source_name
                        , bk.book_cd
                        , bk.trader_name
                        , bk.branch
                        , bk.legal_entity
                        , bk.legal_entity_code
                        , bk.risk_portfolio_name_ru
                        , max(bk.book_modification_date) as book_modification_date
                        , bk.created
                        , bk.modified
                        from prdn_stg.stg.book bk
                          join ##t_max_date_grouped as max_d
                             on bk.book_cd = max_d.book_cd_max
                            and bk.source_name = max_d.source_name_max
                            and bk.book_modification_date = max_d.max_date
                       where bk.wf_load_id = ''' + cast(@wf_load_id as varchar) + '''
                      group by bk.wf_load_id
                             , bk.desk_full_nm
                             , bk.source_name
                             , bk.book_cd
                             , bk.trader_name
                             , bk.branch
                             , bk.legal_entity
                             , bk.legal_entity_code
                             , bk.risk_portfolio_name_ru
                             , bk.created
                             , bk.modified'

    exec dbo.sp_executesql @p_sql

--временная таблица с последними актуальными данными, без дублей
    create table ##stg_brd (
        source_name varchar(10) collate Cyrillic_General_CI_AS not null
      , book_cd varchar(30) collate Cyrillic_General_CI_AS not null
      , wf_load_id numeric(22, 0) not null
      , hash_diff char(32) collate Cyrillic_General_CI_AS not null
      , desk_full_nm varchar(50) collate Cyrillic_General_CI_AS not null
      , trader_name varchar(50) collate Cyrillic_General_CI_AS null
      , branch varchar(10) collate Cyrillic_General_CI_AS null
      , legal_entity varchar(50) collate Cyrillic_General_CI_AS null
      , legal_entity_code varchar(20) collate Cyrillic_General_CI_AS null
      , risk_portfolio_name_ru varchar(300) collate Cyrillic_General_CI_AS null
      , book_modification_date datetime not null
      , created datetime null
      , modified datetime null
      )

    set @p_sql = 'insert into ##stg_brd
                    select
                        bk.source_name
                      , bk.book_cd
                      , bk.wf_load_id
                      , upper(
                              convert(char(32),
                                      hashbytes(''MD5'',
                                               concat(
                                                      case
                                                         when bk.risk_portfolio_name_ru is null and bk.legal_entity_code is null
                                                          and bk.legal_entity is null and bk.branch is null and bk.trader_name is null
                                                          and bk.desk_full_nm is null then ''''
                                                         else '';'' + rtrim(ltrim(isnull(convert(nvarchar, bk.desk_full_nm), '''')))
                                                      end
                                                     ,case
                                                         when bk.risk_portfolio_name_ru is null and bk.legal_entity_code is null
                                                          and bk.legal_entity is null and bk.branch is null and bk.trader_name is null then ''''
                                                         else '';'' + rtrim(ltrim(isnull(convert(nvarchar, bk.trader_name), '''')))
                                                      end
                                                     ,case
                                                         when bk.risk_portfolio_name_ru is null and bk.legal_entity_code is null
                                                          and bk.legal_entity is null and bk.branch is null then ''''
                                                         else '';'' + rtrim(ltrim(isnull(convert(nvarchar, bk.branch), '''')))
                                                      end
                                                     ,case
                                                         when bk.risk_portfolio_name_ru is null and bk.legal_entity_code is null
                                                          and bk.legal_entity is null then ''''
                                                         else '';'' + rtrim(ltrim(isnull(convert(nvarchar, bk.legal_entity), '''')))
                                                      end
                                                     ,case 
                                                         when bk.risk_portfolio_name_ru is null and bk.legal_entity_code is null then ''''
                                                         else '';'' + rtrim(ltrim(isnull(convert(nvarchar, bk.legal_entity_code), '''')))
                                                      end
                                                     ,case
                                                         when bk.risk_portfolio_name_ru is null then ''''
                                                         else '';'' + rtrim(ltrim(isnull(convert(nvarchar, bk.risk_portfolio_name_ru), '''')))
                                                      end
                                               ) -- end concat
                                         ) -- end hashbytes
                                  , 2) -- end convert
                           ) as hash_diff -- end upper
                      , bk.desk_full_nm
                      , bk.trader_name
                      , bk.branch
                      , bk.legal_entity
                      , bk.legal_entity_code
                      , bk.risk_portfolio_name_ru
                      , bk.book_modification_date
                      , bk.created
                      , bk.modified
                      from ##t_max_date as bk'

    exec dbo.sp_executesql @p_sql

    --вставка новых книг

set @p_sql = 'insert into prdn_gmta.rdv.ref_book (
                  source_name
                , book_cd
                , load_dttm
                , wf_load_id
                , src_cd
                , eff_from_dttm
                , eff_to_dttm
                , hash_diff
                , desk_full_nm
                , trader_name
                , branch
                , legal_entity
                , legal_entity_code
                , risk_portfolio_name_ru
                , created
                , modified
            )
              select 
                  stg.source_name
                , stg.book_cd
                , ''' + convert(nvarchar, @curr_date, 121) + '''
                , stg.wf_load_id
                , ''' + @src_cd + '''
                , stg.book_modification_date
                , ''' + convert(nvarchar, @empty_date, 121) + '''
                , stg.hash_diff
                , stg.desk_full_nm
                , stg.trader_name
                , stg.branch
                , stg.legal_entity
                , stg.legal_entity_code
                , stg.risk_portfolio_name_ru
                , stg.created
                , stg.modified
                from ##stg_brd as stg
                  left join prdn_gmta.rdv.ref_book as gmta
                    on upper(rtrim(ltrim(stg.source_name))) = upper(rtrim(ltrim(gmta.source_name)))
                   and upper(rtrim(ltrim(stg.book_cd))) = upper(rtrim(ltrim(gmta.book_cd)))
               where gmta.source_name is null
                 and gmta.book_cd is null'

    exec dbo.sp_executesql @p_sql

    --создаем временную таблицу с обновляемыми строками
    create table ##stg_diff (
        source_name varchar(10) collate Cyrillic_General_CI_AS not null
      , book_cd varchar(30) collate Cyrillic_General_CI_AS not null
      , load_dttm datetime not null
      , wf_load_id numeric(22, 0) not null
      , src_cd varchar(10) collate Cyrillic_General_CI_AS not null
      , eff_from_dttm datetime not null
      , eff_to_dttm datetime not null
      , hash_diff char(32) collate Cyrillic_General_CI_AS not null
      , desk_full_nm varchar(50) collate Cyrillic_General_CI_AS not null
      , trader_name varchar(50) collate Cyrillic_General_CI_AS null
      , branch varchar(10) collate Cyrillic_General_CI_AS null
      , legal_entity varchar(50) collate Cyrillic_General_CI_AS null
      , legal_entity_code varchar(20) collate Cyrillic_General_CI_AS null
      , risk_portfolio_name_ru varchar(300) collate Cyrillic_General_CI_AS null
      , created datetime null
      , modified datetime null
      )

set @p_sql = 'insert into ##stg_diff
                  select 
                      stg.source_name
                    , stg.book_cd
                    , ''' + convert(nvarchar, @curr_date, 121) + '''
                    , stg.wf_load_id
                    , ''' + @src_cd + '''
                    , stg.book_modification_date
                    , ''' + convert(nvarchar, @empty_date, 121) + '''
                    , stg.hash_diff
                    , stg.desk_full_nm
                    , stg.trader_name
                    , stg.branch
                    , stg.legal_entity
                    , stg.legal_entity_code 
                    , stg.risk_portfolio_name_ru 
                    , stg.created
                    , stg.modified
                    from ##stg_brd as stg
                      inner join prdn_gmta.rdv.ref_book as gmta
                         on upper(rtrim(ltrim(stg.source_name))) = upper(rtrim(ltrim(gmta.source_name)))
                        and upper(rtrim(ltrim(stg.book_cd))) = upper(rtrim(ltrim(gmta.book_cd)))
                        and gmta.eff_to_dttm = ''' + convert(nvarchar, @empty_date, 121) + '''
                   where stg.hash_diff != gmta.hash_diff
                     and stg.book_modification_date > gmta.eff_from_dttm'

    exec dbo.sp_executesql @p_sql

    --Закрываем строки по которым изменился hash_diff
set @p_sql = 'update gmta
                  set gmta.eff_to_dttm = dateadd(s, -1 , stg.eff_from_dttm)
                      from prdn_gmta.rdv.ref_book as gmta
                        inner join ##stg_diff as stg
                           on gmta.source_name = stg.source_name
                          and gmta.book_cd = stg.book_cd
                          and gmta.eff_to_dttm = stg.eff_to_dttm'

    exec dbo.sp_executesql @p_sql

    --Вставляем строки по которым изменился hash_diff
set @p_sql = 'insert into prdn_gmta.rdv.ref_book (
                  source_name
                , book_cd
                , load_dttm
                , wf_load_id
                , src_cd
                , eff_from_dttm
                , eff_to_dttm
                , hash_diff
                , desk_full_nm
                , trader_name
                , branch
                , legal_entity
                , legal_entity_code
                , risk_portfolio_name_ru
                , created
                , modified
                )
                  select 
                      source_name
                    , book_cd
                    , load_dttm
                    , wf_load_id
                    , src_cd
                    , eff_from_dttm
                    , eff_to_dttm
                    , hash_diff
                    , desk_full_nm
                    , trader_name
                    , branch
                    , legal_entity
                    , legal_entity_code 
                    , risk_portfolio_name_ru 
                    , created
                    , modified
                    from ##stg_diff'

    exec dbo.sp_executesql @p_sql

    if object_id('tempDB..##t_max_date') is not null
      drop table ##t_max_date
    if object_id('tempDB..##stg_brd') is not null
      drop table ##stg_brd
    if object_id('tempDB..##stg_diff') is not null
      drop table ##stg_diff
    if object_id('tempDB..##t_max_date_grouped') is not null
      drop table ##t_max_date_grouped

commit transaction @p_transact_name

--Проверяем наличие десков
declare @missing_desks numeric --Количество новых десков

select
    @missing_desks = count(distinct gmta.desk_full_nm)
  from prdn_gmta.rdv.ref_book as gmta
left join prdn_gmta.rdv.ref_desk as desk
  on gmta.desk_full_nm = desk.desk_full_nm
 where desk.desk_full_nm is null

if @missing_desks = 0
    begin
        --лог. успешное завершение шага загрузки.
        exec prdn_gmta.tmd.etl_sp_close_wf_load_log
            @wf_load_log_id = @p_wf_load_log_id
          , @status = 'SUCCESS'
          , @description = null
    end
else
    begin
        --лог. успешное завершение шага загрузки.
        set @p_res_descr = 'count of missing desks is ' + cast(@missing_desks as varchar(20))
        exec prdn_gmta.tmd.etl_sp_close_wf_load_log
            @wf_load_log_id = @p_wf_load_log_id
          , @status = 'WARNING'
          , @description = @p_res_descr
    end

    set nocount off
    return 0

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

    if @@trancount > 0 rollback transaction @p_transact_name
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
end
go

grant execute on rdv.sp_traffic_gmta_brd to prdn_r_gmta_tech_loader;
