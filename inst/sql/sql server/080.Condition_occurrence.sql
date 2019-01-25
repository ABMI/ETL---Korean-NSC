/**************************************
 --encoding : UTF-8
 --Author: �̼���
 --Date: 2018.09.11
 
 @NHISNSC_rawdata: DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 @CONDITION_MAPPINGTABLE : mapping table between KCD and SNOMED-CT
 --Description: Condition_occurrence ���̺� ����
 --Generating Table: CONDITION_OCCURRENCE
***************************************/

/**************************************
 1. ���̺� ����
***************************************/ 
/*
CREATE TABLE @NHISNSC_database.CONDITION_OCCURRENCE ( 
     condition_occurrence_id		BIGINT			PRIMARY KEY, 
     person_id						INTEGER			NOT NULL , 
     condition_concept_id			INTEGER			NOT NULL , 
     condition_start_date			DATE			NOT NULL , 
     condition_end_date				DATE, 
     condition_type_concept_id		INTEGER			NOT NULL , 
     stop_reason					VARCHAR(20), 
     provider_id					INTEGER, 
     visit_occurrence_id			BIGINT, 
     condition_source_value			VARCHAR(50),
	 condition_source_concept_id	VARCHAR(50)
);
*/
/**************************************
 1-1. �ӽ� ���� ���̺� ���
***************************************/ 
select a.*, b.invalid_reason as concept_invalid_reason
into #mapping_table
from @NHISNSC_database.source_to_concept_map a join @NHISNSC_database.CONCEPT b on a.target_concept_id=b.concept_id;

update #mapping_table
set invalid_reason=REPLACE(invalid_reason, '', NULL)
, concept_invalid_reason=replace(concept_invalid_reason, '', NULL);

/**************************************
 2. ������ �Է�
    1) ����������: �ڰݳ⵵.01.01�� ����Ʈ. ����⵵�� �� �����̸� ����⵵.01.01
	2) ����������: �ڰݳ⵵.12.31�� ����Ʈ. �������� �� ���ĸ� �����.��.��������
	
	����) 20T: 119,362,188
        40T: 299,379,698
	
	-- checklist
	   1) �� kcdcode full set �ִ��� Ȯ�� -> ������ ������ : �Ϸ�
	   2) condition_type_concept_id �� Ȯ�� -> ������ ������
***************************************/ 
-- observation_period & visiti_occurrence �� �ִ� ������
--((299,311,028), 00:50:39)
INSERT INTO @NHISNSC_database.CONDITION_OCCURRENCE
	(condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date,
	condition_type_concept_id, stop_reason, provider_id, visit_occurrence_id, condition_source_value, 
	condition_source_concept_id)
select
	convert(bigint, convert(varchar, m.master_seq) + convert(varchar, ROW_NUMBER() OVER(partition BY key_seq, seq_no order by target_concept_id desc))) as condition_occurrence_id,
	--ROW_NUMBER() OVER(partition BY key_seq, seq_no order by concept_id desc) AS rank, m.seq_no,
	m.person_id as person_id,
	n.target_concept_id as condition_concept_id,
	convert(date, m.recu_fr_dt, 112) as condition_start_date,
	m.visit_end_date as condition_end_date,
	m.sick_order as condition_type_concept_id,
	null as stop_reason,
	null as provider_id,
	m.key_seq as visit_occurrence_id,
	m.sick_sym as condition_source_value,
	null as condition_source_concept_id
from (
	select
		a.master_seq, a.person_id, a.key_seq, a.seq_no, b.recu_fr_dt,
		case when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt, 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt, 112)) 
			else convert(date, b.recu_fr_dt, 112)
		end as visit_end_date,
		c.sick_sym,
		case when c.SEQ_NO=1 then '44786627'--primary condition
			when c.SEQ_NO=2 then '44786629' --secondary condition
			when c.SEQ_NO=3 then '45756845' --third condition
			when c.SEQ_NO=4 then '45756846'	-- 4th condition
			else '45756847'					-- 5���� ������ ������
		end as sick_order,
		case when b.sub_sick=c.sick_sym then 'Y' else 'N' end as sub_sick_yn
	from (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='140') a, 
		@NHISNSC_rawdata.@NHIS_20T b, --@ó������ߵ�
		@NHISNSC_rawdata.@NHIS_40T c,
		@NHISNSC_database.observation_period d --�߰�
	where a.person_id=b.person_id
	and a.key_seq=b.key_seq
	and a.key_seq=c.key_seq
	and a.seq_no=c.seq_no
	and b.person_id=d.person_id --�߰�
	and convert(date, c.recu_fr_dt, 112) between d.observation_period_start_date and d.observation_period_end_date) as m, --�߰�
	(select * from #mapping_table a where domain_id='condition' and invalid_reason is null and concept_invalid_reason is null) as n
where m.sick_sym=n.source_code;


/********************************************
	2-1. ���ε����ʴ� �Ǽ����� concept_id �� 0 ����
********************************************/
--���� : 326254
INSERT INTO @NHISNSC_database.CONDITION_OCCURRENCE
	(condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date,
	condition_type_concept_id, stop_reason, provider_id, visit_occurrence_id, condition_source_value, 
	condition_source_concept_id)
select
	convert(bigint, convert(varchar, m.master_seq) + convert(varchar, ROW_NUMBER() OVER(partition BY key_seq, seq_no order by target_concept_id desc))) as condition_occurrence_id,
	--ROW_NUMBER() OVER(partition BY key_seq, seq_no order by concept_id desc) AS rank, m.seq_no,
	m.person_id as person_id,
	0 as condition_concept_id,
	convert(date, m.recu_fr_dt, 112) as condition_start_date,
	m.visit_end_date as condition_end_date,
	m.sick_order as condition_type_concept_id,
	null as stop_reason,
	null as provider_id,
	m.key_seq as visit_occurrence_id,
	m.sick_sym as condition_source_value,
	null as condition_source_concept_id

from (
	select
		a.master_seq, a.person_id, a.key_seq, a.seq_no, b.recu_fr_dt,
		case when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt, 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt, 112)) 
			else convert(date, b.recu_fr_dt, 112)
		end as visit_end_date,
		c.sick_sym,
		case when c.SEQ_NO=1 then '44786627'--primary condition
			when c.SEQ_NO=2 then '44786629' --secondary condition
			when c.SEQ_NO=3 then '45756845' --third condition
			when c.SEQ_NO=4 then '45756846'	-- 4th condition
			else '45756847'					-- 5���� ������ ������
		end as sick_order,
		case when b.sub_sick=c.sick_sym then 'Y' else 'N' end as sub_sick_yn
	from (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='140') a, 
		@NHISNSC_rawdata.@NHIS_20T b, --@ó������ߵ�
		@NHISNSC_rawdata.@NHIS_40T c,
		@NHISNSC_database.observation_period d --�߰�
	where a.person_id=b.person_id
	and a.key_seq=b.key_seq
	and a.key_seq=c.key_seq
	and a.seq_no=c.seq_no
	and b.person_id=d.person_id --�߰�
	and convert(date, c.recu_fr_dt, 112) between d.observation_period_start_date and d.observation_period_end_date) as m --�߰�
where m.sick_sym not in (select a.source_code from #mapping_table a where domain_id='condition' and invalid_reason is null and concept_invalid_reason is null);


drop table #mapping_table;
