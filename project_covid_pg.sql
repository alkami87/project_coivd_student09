-- Создаем схему project_coivd_student09
drop schema if exists project_coivd_student09;
create schema if not exists project_coivd_student09;

-- Создаем STG таблицу
drop table if exists project_coivd_student09.stg_covid_data cascade; 
create table project_coivd_student09.stg_covid_data 
( 
    Last_Update timestamp(0)
    , report_dt date 
    , Active_Cases text 
    , Country text 
    , New_Cases text 
    , New_Deaths text 
    , Total_Cases text 
    , Total_Deaths text 
    , Total_Recovered text 
); 
COMMENT on column project_coivd_student09.stg_covid_data.Last_Update is  'дата загрузки данных';
COMMENT on column project_coivd_student09.stg_covid_data.report_dt is  'отчетная дата загрузки данных';
COMMENT on column project_coivd_student09.stg_covid_data.Active_Cases is  'заболевания (активные)';
COMMENT on column project_coivd_student09.stg_covid_data.Country is  'страна';
COMMENT on column project_coivd_student09.stg_covid_data.New_Cases is  'заболевания (новые)';
COMMENT on column project_coivd_student09.stg_covid_data.New_Deaths is  'умерло (новые)';
COMMENT on column project_coivd_student09.stg_covid_data.Total_Cases is  'заболевания (всего)';
COMMENT on column project_coivd_student09.stg_covid_data.Total_Deaths is  'умерло (всего)';
COMMENT on column project_coivd_student09.stg_covid_data.Total_Recovered is  'выздоровевшие (всего)';

-- Создаем справочник стран
drop table if exists project_coivd_student09.ref_covid_country_pk cascade;
create table project_coivd_student09.ref_covid_country_pk
(
    country_id numeric PRIMARY KEY
    , country text
);
-- !!! INDEX создался атоматически при объявлении country_id >>> PRIMARY KEY
-- CREATE UNIQUE INDEX ref_covid_country_pk_pkey ON project_coivd_student09.ref_covid_country_pk USING btree (country_id)

COMMENT on column project_coivd_student09.ref_covid_country_pk.country_id is  'ид страны';
COMMENT on column project_coivd_student09.ref_covid_country_pk.country is  'страна';


-- Создаем DM таблицу
drop table if exists project_coivd_student09.dm_covid_data_fk cascade;
create table project_coivd_student09.dm_covid_data_fk
( 
    report_dt date 
    , country_id numeric
    , Active_Cases numeric 
    , New_Cases numeric
    , New_Deaths numeric
    , Total_Cases numeric
    , Total_Deaths numeric
    , Total_Recovered numeric
    , Last_Update timestamp(0)
    , FOREIGN KEY (country_id) REFERENCES project_coivd_student09.ref_covid_country_pk (country_id)
); 

CREATE INDEX indx01_rep_dt ON project_coivd_student09.dm_covid_data_fk USING btree (report_dt)

COMMENT on column project_coivd_student09.dm_covid_data_fk.report_dt is  'отчетная дата загрузки данных';
COMMENT on column project_coivd_student09.dm_covid_data_fk.Country_id is  'страна';
COMMENT on column project_coivd_student09.dm_covid_data_fk.Active_Cases is  'заболевания (активные)';
COMMENT on column project_coivd_student09.dm_covid_data_fk.New_Cases is  'заболевания (новые)';
COMMENT on column project_coivd_student09.dm_covid_data_fk.New_Deaths is  'умерло (новые)';
COMMENT on column project_coivd_student09.dm_covid_data_fk.Total_Cases is  'заболевания (всего)';
COMMENT on column project_coivd_student09.dm_covid_data_fk.Total_Deaths is  'умерло (всего)';
COMMENT on column project_coivd_student09.dm_covid_data_fk.Total_Recovered is  'выздоровевшие (всего)';
COMMENT on column project_coivd_student09.dm_covid_data_fk.Last_Update is  'дата загрузки данных';

-- процедура перекладки из STG в DM
CREATE OR REPLACE PROCEDURE project_coivd_student09.proc_covid_calc()
	LANGUAGE plpgsql
AS 
$procedure$
	begin
		-- очистка
		delete from project_coivd_student09.dm_covid_data_fk;
		delete from project_coivd_student09.ref_covid_country_pk;

		-- формируем справочник стран
		insert into project_coivd_student09.ref_covid_country_pk
			select row_number()over(order by Country) as Country_id, Country
			from project_coivd_student09.stg_covid_data
			where Country not in ('NAN', 'WORLD')
			group by Country;
	   	analyze project_coivd_student09.ref_covid_country_pk;
	   
		-- формируем нормализованную DM таблицу
		insert into project_coivd_student09.dm_covid_data_fk
		    select report_dt, Country_id, Active_Cases, New_Cases, New_Deaths
				, Total_Cases, Total_Deaths, Total_Recovered, Last_Update
		    from(
		        select row_number()over(partition by t1.report_dt, t2.Country_id order by t1.Last_Update desc) as rn
		            , t1.Last_Update, t1.report_dt, t2.Country_id
		            , case when t1.Active_Cases = '' then '0' else t1.Active_Cases end::numeric as Active_Cases
		            , case when t1.New_Cases = '' then '0' else t1.New_Cases end::numeric as New_Cases
		            , case when t1.New_Deaths = '' then '0' else t1.New_Deaths end::numeric as New_Deaths
		            , case when t1.Total_Cases = '' then '0' else t1.Total_Cases end::numeric as Total_Cases
		            , case when t1.Total_Deaths = '' then '0' else t1.Total_Deaths end::numeric as Total_Deaths
		            , case when t1.Total_Recovered = '' then '0' else t1.Total_Recovered end::numeric as Total_Recovered
		        from project_coivd_student09.stg_covid_data t1 
		        join project_coivd_student09.ref_covid_country_pk t2 on t1.Country = t2.Country
		        where t1.Country not in ('NAN', 'WORLD')
		        ) t
		    where rn = 1;
		analyze project_coivd_student09.dm_covid_data_fk;
	
--		commit; -- commit на стороне python
	   
	END;
$procedure$

-- Создаем представление DM таблицы + справочник стран
drop view if exists project_coivd_student09.v_dm_covid_data_country;
create or replace view project_coivd_student09.v_dm_covid_data_country as
    select t1.report_dt
        , t2.country
        , t1.Active_Cases
        , t1.New_Cases
        , t1.New_Deaths
        , t1.Total_Cases
        , t1.Total_Deaths
        , t1.Total_Recovered
        , t1.Last_Update
        , row_number()over(partition by t2.country order by t1.Last_Update desc) as rn
    from project_coivd_student09.dm_covid_data_fk  t1
    join project_coivd_student09.ref_covid_country_pk t2 on t1.country_id = t2.country_id;
    
-- Агрегат по дня мир для grafana
drop view if exists project_coivd_student09.v_grafan_covid_data_world_daily;
create or replace view project_coivd_student09.v_grafan_covid_data_world_daily as 
	select report_dt
		, sum(active_cases) as active_cases
		, sum(new_cases) as new_cases
		, sum(new_deaths) as new_deaths
		, sum(total_cases) as total_cases
		, sum(total_deaths) as total_deaths
		, sum(total_recovered) as total_recovered
	from project_coivd_student09.v_dm_covid_data_country
	group by report_dt;
	
-- Текущий день мир для grafana
drop view if exists project_coivd_student09.v_grafan_covid_data_world_current_date;
create or replace view project_coivd_student09.v_grafan_covid_data_world_current_date as 
	select sum(active_cases) as active_cases
		, sum(new_cases) as new_cases
		, sum(new_deaths) as new_deaths
		, sum(total_cases) as total_cases
		, sum(total_deaths) as total_deaths
		, sum(total_recovered) as total_recovered
	from project_coivd_student09.v_dm_covid_data_country
	where rn = 1;
	
-- Агрегат по дня Россия для grafana
drop view if exists project_coivd_student09.v_grafan_covid_data_rus_daily;
create or replace view project_coivd_student09.v_grafan_covid_data_rus_daily as 
	select report_dt
		, sum(active_cases) as active_cases
		, sum(new_cases) as new_cases
		, sum(new_deaths) as new_deaths
		, sum(total_cases) as total_cases
		, sum(total_deaths) as total_deaths
		, sum(total_recovered) as total_recovered
	from project_coivd_student09.v_dm_covid_data_country
	where country = 'RUSSIA'
	group by report_dt;

-- Текущий день Россия для grafana
drop view if exists project_coivd_student09.v_grafan_covid_data_rus_current_date;
create or replace view project_coivd_student09.v_grafan_covid_data_rus_current_date as 
	select sum(active_cases) as active_cases
		, sum(new_cases) as new_cases
		, sum(new_deaths) as new_deaths
		, sum(total_cases) as total_cases
		, sum(total_deaths) as total_deaths
		, sum(total_recovered) as total_recovered
	from project_coivd_student09.v_dm_covid_data_country
	where rn = 1
		and country = 'RUSSIA';

