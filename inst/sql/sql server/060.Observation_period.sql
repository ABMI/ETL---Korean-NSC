/**************************************
 --encoding : UTF-8
 --Author: �̼���, ������
 --Date: 2018.09.10
 
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 --Description: Observation_period ���̺� ����
 --Generating Table: OBSERVATION_PERIOD
***************************************/

/**************************************
 1. ������ �Է�
    1) ����������: �ڰݳ⵵.01.01�� ����Ʈ. ����⵵�� �� �����̸� ����⵵.01.01
	2) ����������: �ڰݳ⵵.12.31�� ����Ʈ. �������� �� ���ĸ� �����.��.��������
	3) ��� ���� ������ �ڰ� ����
***************************************/ 
-- step 1
select
      a.person_id as person_id, 
      case when a.stnd_y >= b.year_of_birth then convert(date, convert(varchar, a.stnd_y) + '0101', 112) 
            else convert(date, convert(varchar, b.year_of_birth) + '0101', 112) 
      end as observation_period_start_date, --����������
      case when convert(date, a.stnd_y + '1231', 112) > c.death_date then c.death_date
            else convert(date, a.stnd_y + '1231', 112)
      end as observation_period_end_date --����������
into #observation_period_temp1
from @NHISNSC_rawdata.@NHID_JK a,
      @NHISNSC_database.person b left join @NHISNSC_database.death c
      on b.person_id=c.person_id
where a.person_id=b.person_id

-- step 2
select *, row_number() over(partition by person_id order by observation_period_start_date, observation_period_end_date) AS id
into #observation_period_temp2
from #observation_period_temp1
where observation_period_start_date < observation_period_end_date -- ��� ���� ������ �ڰ��� ���ܽ�Ű�� ����

-- step 3
select 
	a.*, datediff(day, a.observation_period_end_date, b.observation_period_start_date) as days
	into #observation_period_temp3
	from #observation_period_temp2 a
		left join
		#observation_period_temp2 b
		on a.person_id = b.person_id
			and a.id = cast(b.id as int)-1
	order by person_id, id

-- step 4
select
	a.*, CASE WHEN id=1 THEN 1
   ELSE SUM(CASE WHEN DAYS>1 THEN 1 ELSE 0 END) OVER(PARTITION BY person_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)+1
   END AS sumday
   into #observation_period_temp4
   from #observation_period_temp3 a
   order by person_id, id


-- step 5
select identity(int, 1, 1) as observation_period_id,
	person_id,
	min(observation_period_start_date) as observation_period_start_date,
	max(observation_period_end_date) as observation_period_end_date,
	44814725 as PERIOD_TYPE_CONCEPT_ID
INTO @NHISNSC_database.OBSERVATION_PERIOD
from #observation_period_temp4
group by person_id, sumday
order by person_id, observation_period_start_date

drop table #observation_period_temp1, #observation_period_temp2, #observation_period_temp3, #observation_period_temp4
