
drop table  if exists bp_available;
create table bp_available as
select a.*,b.index_date,abs(strftime('%M',b.index_date) - strftime('%M',a.date)) as bp_months
from
    (select distinct pid,date
     from vitals
     where lower(type) in ('sbp','dbp')
    )a
    inner join
    (
        select
            pid,
            min(start_dt) as index_date
        from meds
        where start_dt <> ''
        group by 1
    )b
    on a.pid = b.pid and b.index_date <> ''
;

drop table  if exists a1c_available;
create table a1c_available as
select a.*,b.index_date,strftime('%M',b.index_date) - strftime('%M',a.date) as bp_months
from
    (select distinct pid,date
     from labs
     where lower(type) in ('a1c')
       and result < 6.5
       and pid not in (select distinct pid from dx where categ = 't2dm' and cast(strftime('%Y',date) as int) <=2010)
    )a
inner join
    (select distinct pid from meds where categ not like 'dm_%' and cast(strftime('%Y',date) as int) > 2010)dm_med
    on a.pid = dm_med.pid
        inner join
    (
        select
            pid,
            min(start_dt) as index_date
        from meds
        where start_dt <> ''
        group by 1
    )b
    on a.pid = b.pid and b.index_date <> ''
;

drop table if exists statin_cohort;
create table statin_cohort as
select *
from
    (select
         a.*,
         c.demo_incl,
         cast(strftime('%Y',a.index_date) as integer) as index_year
     from
         (select
              pid,
              min(start_dt) as index_date
          from meds
          where pid in (select distinct pid from bp_available where bp_months <= 12)
            and start_dt <> ''
          group by 1) a
             inner join
         (select distinct pid, 'Y' as demo_incl
          from patient
          where birth_dt is not null
            and sex is not null
            and ethn is not null
            and race is not null
            and (2023 - cast(strftime('%Y',birth_dt) as int) > 30)
            and (death_dt is null or death_dt = '' or lower(dead)<>'d')
         ) c
         on a.pid = c.pid
             inner join
         (
             select distinct pid
             from a1c_available
             where bp_months <= 12
         )a1c
         on a.pid = a1c.pid
    )cohot
where index_year >= 2011
;
--meidan index date for non statin users

DROP TABLE IF EXISTS nonstatin_index_date;
CREATE TABLE nonstatin_index_date AS
SELECT index_date AS median_index_date
FROM (
         SELECT index_date,
                ROW_NUMBER() OVER (ORDER BY index_date) AS rn,
                COUNT(*) OVER () AS cnt
         FROM statin_cohort
     )
WHERE rn = (cnt + 1) / 2 OR rn = (cnt / 2) + 1
ORDER BY index_date
LIMIT 1;

drop table  if exists bp_available_1;
create table bp_available_1 as
select a.*,b.median_index_date,abs(strftime('%M',b.median_index_date) - strftime('%M',a.date)) as bp_months
from
    (select distinct pid,date
     from vitals
     where lower(type) in ('sbp','dbp')
    )a
        inner join
    (
        select
            pid,
            min(start_dt) as median_index_date
        from meds
        where start_dt <> ''
        group by 1
    )b
    on a.pid = b.pid and b.median_index_date <> ''
;

drop table  if exists a1c_available_1;
create table a1c_available_1 as
select a.*,b.index_date,strftime('%M',b.index_date) - strftime('%M',a.date) as bp_months
from
    (select distinct pid,date
     from labs
     where  ((lower(type) in ('a1c') and result < 6.5) or (lower(type) in ('tchol') and result < 200))
       and pid not in
                (select distinct pid from dx where categ = 'dm%' and cast(strftime('%Y',date) as int) <=2010
                union all
                select distinct pid from dx where categ = 'hl%' and cast(strftime('%Y',date) as int) <=2018
               )
    )a
inner join
    (select distinct pid from meds where categ not like 'hl_%' and categ not like 'dm%' and cast(strftime('%Y',date) as int) >2010)dm_med
on a.pid = dm_med.pid
inner join
    (
        select
            pid,
            min(start_dt) as index_date
        from meds
        where start_dt <> ''
        group by 1
    )b
    on a.pid = b.pid and b.index_date <> ''
;

drop table if exists non_statin_cohort;
create table non_statin_cohort as
select *
from
    (select
         a.*,
         c.demo_incl,
         cast(strftime('%Y',a.index_date) as integer) as index_year
     from
         (select
              pid,
              min(start_dt) as index_date
          from meds
          where pid in (select distinct pid from bp_available_1 where bp_months <= 12)
            and start_dt <> ''
          group by 1) a
        inner join
         (select distinct pid, 'Y' as demo_incl
          from patient
          where birth_dt is not null
            and sex is not null
            and ethn is not null
            and race is not null
            and (2023 - cast(strftime('%Y',birth_dt) as int) > 30)
            and (death_dt is null or death_dt = '' or lower(dead)<>'d')
         ) c
         on a.pid = c.pid
             inner join
         (
             select distinct pid
             from a1c_available_1
             where bp_months <= 12
         )a1c
         on a.pid = a1c.pid
    )cohot
where index_year >= 2011;


DROP TABLE IF EXISTS combined_table;

-- Create combined data table
CREATE TABLE combined_table AS
SELECT
    *
FROM statin_cohort
UNION ALL
SELECT
    *
FROM non_statin_cohort;

DROP TABLE IF EXISTS combined_table_c;

CREATE TABLE combined_table_c AS
SELECT *
FROM combined_table
ORDER BY pid
LIMIT 2000;


---Final Table Creation


DROP TABLE IF EXISTS final_data;
CREATE TABLE final_data AS
WITH PatientInfo AS (
    SELECT
        c.pid,
        c.index_date,
        (strftime('%Y', c.index_date) - strftime('%Y', p.birth_dt)) AS Age_Index,
        p.sex,
        p.race,
        MAX(s.status) AS smoker_status, -- Assuming the latest status is desired
        MAX(s.years) AS smoking_years -- Assuming the maximum years value is desired
    FROM combined_table_c c
             JOIN patient p ON c.pid = p.pid
             LEFT JOIN smoking s ON c.pid = s.pid AND s.date <= c.index_date
    GROUP BY c.pid
),
     LabsInfo AS (
         SELECT
             l.pid,
             MAX(CASE WHEN LOWER(l.type) = 'ldl' THEN l.result END) AS max_ldl,
             MAX(CASE WHEN LOWER(l.type) = 'trigl' THEN l.result END) AS max_tg,
             MIN(CASE WHEN LOWER(l.type) = 'hdl' THEN l.result END) AS min_hdl
         FROM labs l
                  JOIN combined_table_c c ON l.pid = c.pid
         WHERE l.date BETWEEN date(c.index_date, '-2 years') AND c.index_date
         GROUP BY l.pid
     ),
     VitalsInfo AS (
         SELECT
             v.pid,
             MAX(CASE WHEN LOWER(v.type) = 'sbp' THEN v.value END) AS max_sbp,
             MAX(CASE WHEN LOWER(v.type) = 'dbp' THEN v.value END) AS max_dbp,
             MAX(CASE WHEN LOWER(v.type) = 'pulse' THEN v.value END) AS max_pulse,
             MAX(CASE WHEN LOWER(v.type) = 'bmi' THEN v.value END) AS max_bmi
         FROM vitals v
                  JOIN combined_table_c c ON v.pid = c.pid
         WHERE v.date BETWEEN date(c.index_date, '-2 years') AND c.index_date
         GROUP BY v.pid
     ),
     MedsInfo AS (
         SELECT
             m.pid,
             COUNT(DISTINCT m.drug_name) AS Meds_Count,
             CASE
                 WHEN LOWER(m.categ) = 'hl_statin' THEN 'Statin'
                 WHEN LOWER(m.categ) LIKE 'hl%' THEN 'Non-Statin'
                 ELSE 'non-htn'
                 END AS med
         FROM meds m
                  JOIN combined_table_c c ON m.pid = c.pid
         WHERE m.date BETWEEN date(c.index_date, '-2 years') AND c.index_date
         GROUP BY m.pid
     ),
     OutcomeInfo AS (
         SELECT
             c.pid,
             MAX(CASE WHEN LOWER(d.categ) = 't2dm' AND d.date BETWEEN c.index_date AND date(c.index_date, '+5 years') THEN 1 ELSE 0 END) AS has_t2dm,
             MAX(CASE WHEN LOWER(l.type) = 'a1c' AND l.result > 6.5 AND l.date BETWEEN c.index_date AND date(c.index_date, '+5 years') THEN l.result ELSE 0 END) AS max_a1c
         FROM combined_table_c c
                  LEFT JOIN dx d ON c.pid = d.pid
                  LEFT JOIN labs l ON c.pid = l.pid
         GROUP BY c.pid
     ),
     FinalOutcome AS (
         SELECT
             c.pid,
             CASE
                 WHEN o.has_t2dm = 1 OR o.max_a1c > 6.5 THEN '1'
                 WHEN p.dead = 0 AND p.death_dt > date(c.index_date, '+5 years') THEN '0'
                 ELSE '9'
                 END AS outcome_status
         FROM combined_table_c c
                  JOIN OutcomeInfo o ON c.pid = o.pid
                  JOIN patient p ON c.pid = p.pid
         GROUP BY c.pid
     )
SELECT
    pi.pid,
    pi.Age_Index,
    pi.sex,
    pi.race,
    pi.smoker_status,
    pi.smoking_years,
    li.max_ldl,
    li.max_tg,
    li.min_hdl,
    vi.max_sbp,
    vi.max_dbp,
    vi.max_pulse,
    vi.max_bmi,
    mi.Meds_Count,
    mi.med,
    fo.outcome_status
FROM PatientInfo pi
         LEFT JOIN LabsInfo li ON pi.pid = li.pid
         LEFT JOIN VitalsInfo vi ON pi.pid = vi.pid
         LEFT JOIN MedsInfo mi ON pi.pid = mi.pid
         LEFT JOIN FinalOutcome fo ON pi.pid = fo.pid
GROUP BY pi.pid;


--Create Individual Tables
DROP TABLE IF EXISTS PatientInfo;
CREATE TABLE PatientInfo AS
SELECT
    c.pid,
    c.index_date,
    (strftime('%Y', c.index_date) - strftime('%Y', p.birth_dt)) AS Age_Index,
    p.sex,
    p.race,
    MAX(s.status) AS smoker_status, -- Assuming the latest status is desired
    MAX(s.years) AS smoking_years -- Assuming the maximum years value is desired
FROM combined_table c
         JOIN patient p ON c.pid = p.pid
         LEFT JOIN smoking s ON c.pid = s.pid AND s.date <= c.index_date
GROUP BY c.pid;


DROP TABLE if exists LabsInfo;
CREATE TABLE LabsInfo AS
SELECT
    l.pid,
    MAX(CASE WHEN LOWER(l.type) = 'ldl' THEN l.result END) AS max_ldl,
    MAX(CASE WHEN LOWER(l.type) = 'trigl' THEN l.result END) AS max_tg,
    MIN(CASE WHEN LOWER(l.type) = 'hdl' THEN l.result END) AS min_hdl
FROM labs l
         JOIN combined_table c ON l.pid = c.pid
WHERE l.date BETWEEN date(c.index_date, '-2 years') AND c.index_date
GROUP BY l.pid;


DROP TABLE if exists MedsInfo;
CREATE TABLE MedsInfo AS
SELECT
    m.pid,
    COUNT(DISTINCT m.drug_name) AS Meds_Count,
    CASE
        WHEN LOWER(m.categ) = 'hl_statin' THEN 'Statin'
        WHEN LOWER(m.categ) LIKE 'hl%' THEN 'Non-Statin'
        ELSE 'non-htn'
        END AS med
FROM meds m
         JOIN combined_table c ON m.pid = c.pid
WHERE m.date BETWEEN date(c.index_date, '-2 years') AND c.index_date
GROUP BY m.pid;


DROP TABLE if exists VitalsInfo;
CREATE TABLE VitalsInfo AS
SELECT
    v.pid,
    MAX(CASE WHEN LOWER(v.type) = 'sbp' THEN v.value END) AS max_sbp,
    MAX(CASE WHEN LOWER(v.type) = 'dbp' THEN v.value END) AS max_dbp,
    MAX(CASE WHEN LOWER(v.type) = 'pulse' THEN v.value END) AS max_pulse,
    MAX(CASE WHEN LOWER(v.type) = 'bmi' THEN v.value END) AS max_bmi
FROM vitals v
         JOIN combined_table c ON v.pid = c.pid
WHERE v.date BETWEEN date(c.index_date, '-2 years') AND c.index_date
GROUP BY v.pid;


DROP TABLE IF EXISTS has_t2dmInfo;
CREATE TABLE has_t2dmInfo AS
SELECT
    c.pid,
    MAX(CASE WHEN LOWER(d.categ) = 't2dm' AND d.date BETWEEN c.index_date AND strftime('%Y-%m-%d', c.index_date, '+5 years') THEN 1 ELSE 0 END) AS has_t2dm
FROM combined_table c
         LEFT JOIN dx d ON c.pid = d.pid
GROUP BY c.pid;


DROP TABLE IF EXISTS max_a1cInfo;
CREATE TABLE max_a1cInfo AS
SELECT
    c.pid,
    MAX(CASE WHEN LOWER(l.type) = 'a1c' AND l.result > 6.5 AND l.date BETWEEN c.index_date AND strftime('%Y-%m-%d', c.index_date, '+5 years') THEN l.result ELSE 0 END) AS max_a1c
FROM combined_table c
         LEFT JOIN labs l ON c.pid = l.pid
GROUP BY c.pid;

DROP TABLE IF EXISTS db_medInfo;
CREATE TABLE db_medInfo AS
SELECT
    c.pid,
    MAX(CASE WHEN LOWER(m.categ) LIKE 'dm%' AND m.start_dt BETWEEN c.index_date AND strftime('%Y-%m-%d', c.index_date, '+5 years') THEN 1 ELSE 0 END) AS db_med
FROM combined_table c
         LEFT JOIN meds m ON c.pid = m.pid
GROUP BY c.pid;

DROP TABLE IF EXISTS OutcomeInfo;

CREATE TABLE OutcomeInfo AS
SELECT
    t1.pid,
    t1.has_t2dm,
    t2.max_a1c,
    t3.db_med
FROM has_t2dmInfo t1
         JOIN max_a1cInfo t2 ON t1.pid = t2.pid
         JOIN db_medInfo t3 ON t1.pid = t3.pid;


DROP TABLE IF EXISTS FinalOutcome;
CREATE TABLE FinalOutcome AS
SELECT
    c.pid,
    CASE
        WHEN o.has_t2dm = 1 OR o.max_a1c > 6.5 OR db_med =1 THEN '1'
        WHEN p.dead = 0 AND p.death_dt > date(c.index_date, '+5 years') THEN '0'
        ELSE '9'
        END AS outcome
FROM combined_table c
         JOIN OutcomeInfo o ON c.pid = o.pid
         JOIN patient p ON c.pid = p.pid
GROUP BY c.pid;


-- First, create a table with all unique pids from all tables
DROP TABLE IF EXISTS AllPids;
CREATE TABLE AllPids AS
SELECT pid FROM db_medInfo
UNION
SELECT pid FROM LabsInfo
UNION
SELECT pid FROM MedsInfo
UNION
SELECT pid FROM VitalsInfo
UNION
SELECT pid FROM OutcomeInfo
UNION
SELECT pid FROM FinalOutcome;

-- Now, create the Final_data table by left joining all tables on the AllPids table
DROP TABLE IF EXISTS Final_data;
CREATE TABLE Final_data AS
SELECT
    p.pid,
    db.db_med,
    lb.max_ldl,
    lb.max_tg,
    lb.min_hdl,
    md.Meds_Count,
    md.med,
    vt.max_sbp,
    vt.max_dbp,
    vt.max_pulse,
    vt.max_bmi,
    oc.has_t2dm,
    oc.max_a1c,
    oc.db_med,
    fo.outcome
FROM AllPids p
         LEFT JOIN db_medInfo db ON p.pid = db.pid
         LEFT JOIN LabsInfo lb ON p.pid = lb.pid
         LEFT JOIN MedsInfo md ON p.pid = md.pid
         LEFT JOIN VitalsInfo vt ON p.pid = vt.pid
         LEFT JOIN OutcomeInfo oc ON p.pid = oc.pid
         LEFT JOIN FinalOutcome fo ON p.pid = fo.pid;
