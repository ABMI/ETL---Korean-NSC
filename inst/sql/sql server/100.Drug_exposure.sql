﻿/**************************************
 --encoding : UTF-8
 --Author: 이성원
 --Date: 2018.09.11
 
@NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
@NHISNSC_database: DB for NHIS-NSC in CDM format
@Mapping_database : DB for mapping table
@NHIS_JK: JK table in NHIS NSC
@@NHIS_20T: 20 table in NHIS NSC
@NHIS_30T: 30 table in NHIS NSC
@NHIS_40T: 40 table in NHIS NSC
@@NHIS_60T: 60 table in NHIS NSC
@NHIS_GJ: GJ table in NHIS NSC
@CONDITION_MAPPINGTABLE : mapping table between KCD and OMOP vocabulary
@DRUG_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 
 --Description: Drug_exposure 테이블 생성
			   * 30T(진료), 60T(처방전) 테이블에서 각각 ETL을 수행해야 함
 --Generating Table: DRUG_EXPOSURE
***************************************/
/**************************************
 1. 사전 준비
***************************************/ 
/*
-- 1) 30T의 항/목 코드 현황 체크매핑
select clause_cd, item_cd, count(clause_cd)
from @NHISNSC_rawdata.@NHIS_30T
group by clause_cd, item_cd
--> 결과는 "08. 참고) 30T, 60T의 코드 분석.xlsx" 참고


-- 2) 30T의 계산식에 들어갈 숫자 데이터 정합성 체크
-- 1일 투여량 또는 실시 횟수
select dd_mqty_exec_freq, count(dd_mqty_exec_freq) as cnt
from @NHISNSC_rawdata.@NHIS_30T
where dd_mqty_exec_freq is not null and ISNUMERIC(dd_mqty_exec_freq) = 0
group by dd_mqty_exec_freq


-- 총투여일수 또는 실시횟수
select mdcn_exec_freq, count(mdcn_exec_freq) as cnt
from @NHISNSC_rawdata.@NHIS_30T
where mdcn_exec_freq is not null and ISNUMERIC(mdcn_exec_freq) = 0
group by mdcn_exec_freq


-- 1회 투약량
select dd_mqty_freq, count(dd_mqty_freq) as cnt
from @NHISNSC_rawdata.@NHIS_30T
where dd_mqty_freq is not null and ISNUMERIC(dd_mqty_freq) = 0
group by dd_mqty_freq
--> 결과는 "08. 참고) 30T, 60T의 코드 분석.xlsx" 참고


-- 3) 60T의 계산식에 들어갈 숫자 데이터 정합성 체크
-- 1회 투약량
select dd_mqty_freq, count(dd_mqty_freq) as cnt
from @NHISNSC_rawdata.@@NHIS_60T
where dd_mqty_freq is not null and ISNUMERIC(dd_mqty_freq) = 0
group by dd_mqty_freq

-- 1일 투약량
select dd_exec_freq, count(dd_exec_freq) as cnt
from @NHISNSC_rawdata.@@NHIS_60T
where dd_exec_freq is not null and ISNUMERIC(dd_exec_freq) = 0
group by dd_exec_freq

-- 총투여일수 또는 실시횟수
select mdcn_exec_freq, count(mdcn_exec_freq) as cnt
from @NHISNSC_rawdata.@@NHIS_60T
where mdcn_exec_freq is not null and ISNUMERIC(mdcn_exec_freq) = 0
group by mdcn_exec_freq
--> 결과는 "08. 참고) 30T, 60T의 코드 분석.xlsx" 참고


-- 4) 매핑 테이블의 약코드 1:N 건수 체크
select source_code, count(source_code)
from   (select source_code from @NHISNSC_database.source_to_concept_map where domain_id='Drug' and invalid_reason is null) a
group by source_code
having count(source_code)>1
--> 1:N 매핑 약코드 없음


--몇 건이나 늘어날지 예측
--30T
select count(*) from @NHISNSC_rawdata.@NHIS_30T
where div_cd in (select source_code
				from   (select source_code from @NHISNSC_database.source_to_concept_map where domain_id='Drug' and invalid_reason is null) a
				group by source_code
				having count(source_code)>1)
--60T
select count(*) from @NHISNSC_rawdata.@@NHIS_60T
where div_cd in (select source_code
				from   (select source_code from @NHISNSC_database.source_to_concept_map where domain_id='Drug' and invalid_reason is null) a
				group by source_code
				having count(source_code)>1)

--비맵핑건수 파악
--30T
select count(*) from @NHISNSC_rawdata.@NHIS_30T
where DIV_CD not in (
select DIV_CD from @NHISNSC_rawdata.@@NHIS_30Ta, (select * from @NHISNSC_database.source_to_concept_map where domain_id='drug' and invalid_reason is null) b
where a.DIV_CD=b.source_code
)

--60T
select count(*) from @NHISNSC_rawdata.@@NHIS_60T
where DIV_CD not in (
select DIV_CD from @NHISNSC_rawdata.@@NHIS_60T a, (select * from @NHISNSC_database.source_to_concept_map where domain_id='drug' and invalid_reason is null) b
where a.DIV_CD=b.source_code
)


-- 5) 변환 예상 건수 파악
--30T의 변환예상 건수
select count(a.key_seq)
from @NHISNSC_rawdata.@@NHIS_30Ta, 
	(select source_code
	from @NHISNSC_database.source_to_concept_map
	where domain_id='drug' and invalid_reason is null ) as b, 
	@NHISNSC_rawdata.NHID_GY20_T1 c
where a.div_cd=b.source_code
and a.key_seq=c.key_seq

--60T의 변환예상 건수
select count(a.key_seq)
from @NHISNSC_rawdata.@@NHIS_60T a, 
	(select source_code
	from @NHISNSC_database.source_to_concept_map 
	where domain_id='drug' and invalid_reason is null) b, 
	@NHISNSC_rawdata.NHID_GY20_T1 c
where a.div_cd=b.source_code
and a.key_seq=c.key_seq

*/

/**************************************
 1.1. drug_exposure_end_date 계산 방법을 정하기 위해 실행한 쿼리들 (2017.02.17 by 유승찬)
***************************************/ 
-- observation period 범위 밖의 건수
/*

select a.person_id, a.drug_exposure_id, a.drug_exposure_start_date, a.drug_exposure_end_date, b.observation_period_start_date, b.observation_period_end_date, c.death_date
from @NHISNSC_database.drug_exposure a, @NHISNSC_database.observation_period b, @NHISNSC_database.DEATH C
where a.person_id=b.person_id
and a.person_id = c.person_id
and (a.drug_exposure_start_date < b.observation_period_start_date
or a.drug_exposure_end_date > b.observation_period_end_date)

select b.concept_name, x.*
from 
(select A.*, B.target_concept_id
from @NHISNSC_rawdata.@@NHIS_30TAS A
join( select * from @NHISNSC_database.source_to_concept_map where domain_id='drug') as B
on A.div_cd=b.source_code 
   where cast(DD_MQTY_EXEC_FREQ as float)<1
   and cast(DD_MQTY_EXEC_FREQ as float)>=0) x
   join @NHISNSC_database.concept b
   on x.target_concept_id= b.concept_id

select b.concept_name, x.*
from 
(select A.*, B.target_concept_id
from @NHISNSC_rawdata.@@NHIS_30TAS A
join (select * from @NHISNSC_database.source_to_concept_map where domain_id='drug') as B
on A.div_cd=b.source_code 
   where cast(DD_MQTY_EXEC_FREQ as float)>1) x
   join @NHISNSC_database.CONCEPT b
   on x.target_concept_id= b.concept_id


select b.concept_name, x.*
from 
(select A.*, B.target_concept_id
from @NHISNSC_rawdata.@@NHIS_60T AS A
join (select * from @NHISNSC_database.source_to_concept_map where domain_id='drug') as B
on A.div_cd=b.source_code 
   where cast(DD_MQTY_FREQ as float)>1) x
   join @NHISNSC_database.concept b
   on x.target_concept_id= b.concept_id

 */

/**************************************
 2. 테이블 생성
***************************************/  
/*
CREATE TABLE @NHISNSC_database.DRUG_EXPOSURE ( 
     drug_exposure_id				BIGINT	 	NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     drug_concept_id				INTEGER			NULL , 
     drug_exposure_start_date		DATE			NOT NULL , 
     drug_exposure_end_date			DATE			NULL , 
     drug_type_concept_id			INTEGER			NOT NULL , 
     stop_reason					VARCHAR(20)		NULL , 
     refills						INTEGER			NULL , 
     quantity						FLOAT			NULL , 
     days_supply					INTEGER			NULL , 
     sig							VARCHAR(MAX)	NULL , 
	 route_concept_id				INTEGER			NULL ,
	 effective_drug_dose			FLOAT			NULL ,
	 dose_unit_concept_id			INTEGER			NULL ,
	 lot_number						VARCHAR(50)		NULL ,
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			BIGINT			NULL , 
     drug_source_value				VARCHAR(50)		NULL ,
	 drug_source_concept_id			INTEGER			NULL ,
	 route_source_value				VARCHAR(50)		NULL ,
	 dose_unit_source_value			VARCHAR(50)		NULL
    );
*/	

/**************************************
 2-1. 임시 매핑 테이블 사용
***************************************/ 
select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
into #mapping_table
from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
where a.invalid_reason='' and b.invalid_reason='' and a.domain_id='drug';


/**************************************
 3-1. 30T를 이용하여 데이터 입력
***************************************/  
insert into @NHISNSC_database.DRUG_EXPOSURE 
(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, 
drug_type_concept_id, stop_reason, refills, quantity, days_supply, 
sig, route_concept_id, effective_drug_dose, dose_unit_concept_id, lot_number,
provider_id, visit_occurrence_id, drug_source_value, drug_source_concept_id, route_source_value, 
dose_unit_source_value)
SELECT convert(bigint, convert(varchar, a.master_seq) + convert(varchar, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as drug_exposure_id,
	a.person_id as person_id,
	b.target_concept_id as drug_concept_id,
	CONVERT(date, a.recu_fr_dt, 112) as drug_exposure_start_date,
	--DATEADD(day, CEILING(convert(float, a.mdcn_exec_freq)/convert(float, a.dd_mqty_exec_freq))-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date, (수정: 2017.02.17 by 이성원)
	DATEADD(day, convert(float, a.mdcn_exec_freq)-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date,
	case when a.FORM_CD in ('02', '2', '04', '06', '10', '12') then 38000180 
		when a.FORM_CD not in ('02', '2', '04', '06', '10', '12') then 581452 
		end as drug_type_concept_id, 
	NULL as stop_reason,
	NULL as refills,
	convert(float, a.dd_mqty_exec_freq) * convert(float, a.mdcn_exec_freq) * convert(float, a.dd_mqty_freq) as quantity,
	a.mdcn_exec_freq as days_supply,
	a.clause_cd as sig,
	CASE 
		WHEN a.clause_cd='03' and a.item_cd='01' then 4128794 -- oral
		WHEN a.clause_cd='03' and a.item_cd='02' then 45956875 -- not applicable
		WHEN a.clause_cd='04' and a.item_cd='01' then 4139962 -- Subcutaneous
		WHEN a.clause_cd='04' and a.item_cd='02' then 4112421 -- intravenous
		WHEN a.clause_cd='04' and a.item_cd='03' then 4112421
		ELSE 0
	END as route_concept_id,
	NULL as effective_drug_dose,
	NULL as dose_unit_concept_id,
	NULL as lot_number,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as drug_source_value,
	null as drug_source_concept_id,
	a.clause_cd + '/' + a.item_cd as route_source_value,
	NULL as dose_unit_source_value
FROM 
	(SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd,
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and isnumeric(x.dd_mqty_exec_freq)=1 and cast(x.dd_mqty_exec_freq as float) > '0' then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			case when x.clause_cd is not null and len(x.clause_cd) = 1 and isnumeric(x.clause_cd)=1 and convert(int, x.clause_cd) between 1 and 9 then '0' + x.clause_cd else x.clause_cd end as clause_cd,
			case when x.item_cd is not null and len(x.item_cd) = 1 and isnumeric(x.item_cd)=1 and convert(int, x.item_cd) between 1 and 9 then '0' + x.item_cd else x.item_cd end as item_cd,
			y.master_seq, y.person_id			
	FROM @NHISNSC_rawdata.@NHIS_30T x, 
	     (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='130') y
		, (select form_cd, KEY_SEQ, PERSON_ID from @NHISNSC_rawdata.@NHIS_20T) z
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no
	and y.key_seq=z.KEY_SEQ
	and y.person_id=z.PERSON_ID	) a,
	#mapping_table  b
where a.div_cd=b.source_code
;

/**************************************
 3-2. 60T를 이용하여 데이터 입력
***************************************/
insert into @NHISNSC_database.DRUG_EXPOSURE 
(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, 
drug_type_concept_id, stop_reason, refills, quantity, days_supply, 
sig, route_concept_id, effective_drug_dose, dose_unit_concept_id, lot_number,
provider_id, visit_occurrence_id, drug_source_value, drug_source_concept_id, route_source_value, 
dose_unit_source_value)
SELECT convert(bigint, convert(varchar, a.master_seq) + convert(varchar, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as drug_exposure_id,
	a.person_id as person_id,
	b.target_concept_id as drug_concept_id,
	CONVERT(date, a.recu_fr_dt, 112) as drug_exposure_start_date,
	-- DATEADD(day, CEILING(convert(float, a.mdcn_exec_freq)/convert(float, a.dd_exec_freq))-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date, (수정: 2017.02.17 by 이성원)
	DATEADD(day, convert(float, a.mdcn_exec_freq)-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date,
	case when a.FORM_CD in ('02', '2', '04', '06', '10', '12') then 38000180 
		when a.FORM_CD not in ('02', '2', '04', '06', '10', '12') then 581452 
		end as drug_type_concept_id, 
	NULL as stop_reason,
	NULL as refills,
	convert(float, a.dd_mqty_freq) * convert(float, a.dd_exec_freq) * convert(float, a.mdcn_exec_freq) as quantity,
	a.mdcn_exec_freq as days_supply,
	null as sig,
	null as route_concept_id,
	NULL as effective_drug_dose,
	NULL as dose_unit_concept_id,
	NULL as lot_number,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as drug_source_value,
	null as drug_source_concept_id,
	null as route_source_value,
	NULL as dose_unit_source_value
FROM 
	(SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd,
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			case when x.dd_exec_freq is not null and isnumeric(x.dd_exec_freq)=1 and cast(x.dd_exec_freq as float) > '0' then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			y.master_seq, y.person_id			
	FROM @NHISNSC_rawdata.@NHIS_60T x, 
	     (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='160') y
	, (select form_cd, KEY_SEQ, PERSON_ID from @NHISNSC_rawdata.@NHIS_20T) z
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no
	and y.key_seq=z.KEY_SEQ
	and y.person_id=z.PERSON_ID	) a,
	#mapping_table b
where a.div_cd=b.source_code
;

/**************************************
 3-3. 매핑테이블과 조인되지 않는 30T 데이터 입력
***************************************/  
insert into @NHISNSC_database.DRUG_EXPOSURE 
(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, 
drug_type_concept_id, stop_reason, refills, quantity, days_supply, 
sig, route_concept_id, effective_drug_dose, dose_unit_concept_id, lot_number,
provider_id, visit_occurrence_id, drug_source_value, drug_source_concept_id, route_source_value, 
dose_unit_source_value)
SELECT convert(bigint, convert(varchar, a.master_seq) + convert(varchar, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as drug_exposure_id,
	a.person_id as person_id,
	0 as drug_concept_id,
	CONVERT(date, a.recu_fr_dt, 112) as drug_exposure_start_date,
	--DATEADD(day, CEILING(convert(float, a.mdcn_exec_freq)/convert(float, a.dd_mqty_exec_freq))-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date, (수정: 2017.02.17 by 이성원)
	DATEADD(day, convert(float, a.mdcn_exec_freq)-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date,
	case when a.FORM_CD in ('02', '2', '04', '06', '10', '12') then 38000180 
		when a.FORM_CD not in ('02', '2', '04', '06', '10', '12') then 581452 
		end as drug_type_concept_id, 
	NULL as stop_reason,
	NULL as refills,
	convert(float, a.dd_mqty_exec_freq) * convert(float, a.mdcn_exec_freq) * convert(float, a.dd_mqty_freq) as quantity,
	a.mdcn_exec_freq as days_supply,
	a.clause_cd as sig,
	CASE 
		WHEN a.clause_cd='03' and a.item_cd='01' then 4128794 -- oral
		WHEN a.clause_cd='03' and a.item_cd='02' then 45956875 -- not applicable
		WHEN a.clause_cd='04' and a.item_cd='01' then 4139962 -- Subcutaneous
		WHEN a.clause_cd='04' and a.item_cd='02' then 4112421 -- intravenous
		WHEN a.clause_cd='04' and a.item_cd='03' then 4112421
		ELSE 0
	END as route_concept_id,
	NULL as effective_drug_dose,
	NULL as dose_unit_concept_id,
	NULL as lot_number,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as drug_source_value,
	null as drug_source_concept_id,
	a.clause_cd + '/' + a.item_cd as route_source_value,
	NULL as dose_unit_source_value
FROM 
	(SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd,
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and isnumeric(x.dd_mqty_exec_freq)=1 and cast(x.dd_mqty_exec_freq as float) > '0' then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			case when x.clause_cd is not null and len(x.clause_cd) = 1 and isnumeric(x.clause_cd)=1 and convert(int, x.clause_cd) between 1 and 9 then '0' + x.clause_cd else x.clause_cd end as clause_cd,
			case when x.item_cd is not null and len(x.item_cd) = 1 and isnumeric(x.item_cd)=1 and convert(int, x.item_cd) between 1 and 9 then '0' + x.item_cd else x.item_cd end as item_cd,
			y.master_seq, y.person_id			
	FROM @NHISNSC_rawdata.@NHIS_30T x, 
	     (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='130') y
		, (select form_cd, KEY_SEQ, PERSON_ID from @NHISNSC_rawdata.@NHIS_20T) z
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no
	and y.key_seq=z.KEY_SEQ
	and y.person_id=z.PERSON_ID	) a
where a.div_cd not in (select source_code from #mapping_table )
;

/**************************************
 3-4. 매핑테이블과 조인되지 않는 60T 데이터 입력
***************************************/
insert into @NHISNSC_database.DRUG_EXPOSURE 
(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, 
drug_type_concept_id, stop_reason, refills, quantity, days_supply, 
sig, route_concept_id, effective_drug_dose, dose_unit_concept_id, lot_number,
provider_id, visit_occurrence_id, drug_source_value, drug_source_concept_id, route_source_value, 
dose_unit_source_value)
SELECT convert(bigint, convert(varchar, a.master_seq) + convert(varchar, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as drug_exposure_id,
	a.person_id as person_id,
	0 as drug_concept_id,
	CONVERT(date, a.recu_fr_dt, 112) as drug_exposure_start_date,
	-- DATEADD(day, CEILING(convert(float, a.mdcn_exec_freq)/convert(float, a.dd_exec_freq))-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date, (수정: 2017.02.17 by 이성원)
	DATEADD(day, convert(float, a.mdcn_exec_freq)-1, convert(date, a.recu_fr_dt, 112)) as drug_exposure_end_date,
	case when a.FORM_CD in ('02', '2', '04', '06', '10', '12') then 38000180 
		when a.FORM_CD not in ('02', '2', '04', '06', '10', '12') then 581452 
		end as drug_type_concept_id, 
	NULL as stop_reason,
	NULL as refills,
	convert(float, a.dd_mqty_freq) * convert(float, a.dd_exec_freq) * convert(float, a.mdcn_exec_freq) as quantity,
	a.mdcn_exec_freq as days_supply,
	null as sig,
	null as route_concept_id,
	NULL as effective_drug_dose,
	NULL as dose_unit_concept_id,
	NULL as lot_number,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as drug_source_value,
	null as drug_source_concept_id,
	null as route_source_value,
	NULL as dose_unit_source_value
FROM 
	(SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd,
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			case when x.dd_exec_freq is not null and isnumeric(x.dd_exec_freq)=1 and cast(x.dd_exec_freq as float) > '0' then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			y.master_seq, y.person_id			
	FROM @NHISNSC_rawdata.@NHIS_60T x, 
	     (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='160') y
	, (select form_cd, KEY_SEQ, PERSON_ID from @NHISNSC_rawdata.@NHIS_20T) z
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no
	and y.key_seq=z.KEY_SEQ
	and y.person_id=z.PERSON_ID	) a
where a.div_cd not in (select source_code from #mapping_table )
;

drop table #mapping_table;

/**************************************
 5. drug_start_date가 사망일자 이전인 데이터 삭제
***************************************/
delete from a 
from @NHISNSC_database.DRUG_EXPOSURE a, @NHISNSC_database.death b
where a.person_id=b.person_id
and b.death_date < a.drug_exposure_start_date



/**************************************
 6. drug_end_date가 사장일자 이전인 데이터의 drug_end_date를 사망일자로 변경
***************************************/
update a
set drug_exposure_end_date=b.death_date
from @NHISNSC_database.DRUG_EXPOSURE a, @NHISNSC_database.DEATH b
where a.person_id=b.person_id
and (b.death_date < a.drug_exposure_start_date
or b.death_date < a.drug_exposure_end_date)

/*
-------------------------------------------
참고) http://tennesseewaltz.tistory.com/236
UPDATE A
      SET A.SEQ     = B.CMT_NO
        , A.CarType = B.CAR_TYPE
     FROM TABLE_AAA A
          JOIN TABLE_BBB B ON A.OPCode = B.OP_CODE
    WHERE A.LineCode = '조건'
-------------------------------------------
*/