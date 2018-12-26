/**************************************
 --encoding : UTF-8
 --Author: OHDSI
  
@NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
@NHISNSC_database : DB for NHIS-NSC in CDM format
 
 --Description: OHDSI���� ������ condition_era ���� ����
               �� 3���� temp table�� �������
			   1) #cteConditionTarget
			   2) #cteEndDates
			   3) #cteConditionEnds
 --Generating Table: CONDITION_ERA
***************************************/

/**************************************
 @Author: OHDSI
 @Date: 2017.02.21 ����
 
 @Database: @NHISNSC_database 
 @Description: OHDSI���� ������ condition_era ���� ����
               �� 3���� temp table�� �������
			   1) #cteConditionTarget
			   2) #cteEndDates
			   3) #cteConditionEnds
***************************************/

/**************************************
 1. condition_era ���̺� ����
***************************************/ 
CREATE TABLE nhis_nsc_new.dbo.CONDITION_ERA_cpt4  (
     condition_era_id					INTEGER	 identity(1,1)    NOT NULL , 
     person_id							INTEGER     NOT NULL ,
     condition_concept_id				INTEGER   NOT NULL ,
     condition_era_start_date			DATE      NOT NULL ,
     condition_era_end_date				DATE 	  NOT NULL ,
     condition_occurrence_count			INTEGER			NULL 
); 


/**************************************
 2. 1�ܰ�: �ʿ� ������ ��ȸ
***************************************/ 
--------------------------------------------#cteConditionTarget
SELECT
	condition_occurrence_id, 
	person_id, 
	condition_concept_id, 
	condition_start_date, 
	COALESCE(NULLIF(condition_end_date,NULL), dateadd (day, 31, condition_start_date)) AS condition_end_date
into #cteConditionTarget 
FROM nhis_nsc_new.dbo.CONDITION_OCCURRENCE_cpt4;
	
	
--------------------------------------------#cteEndDates
SELECT
	person_id
	, condition_concept_id
	, dateadd(day, -30, event_date) AS end_date 
into #cteEndDates FROM
(
	SELECT
		person_id
		, condition_concept_id
		, event_date
		, event_type
		, MAX(start_ordinal) OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with 
		, ROW_NUMBER() OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
	FROM
	(
		-- select the start dates, assigning a row number to each
		SELECT
			person_id
			, condition_concept_id
			, condition_start_date AS event_date
			, -1 AS event_type
			, ROW_NUMBER() OVER (PARTITION BY person_id, condition_concept_id ORDER BY condition_start_date) AS start_ordinal
		FROM #cteConditionTarget
	
		UNION ALL
	
		-- pad the end dates by 30 to allow a grace period for overlapping ranges.
		SELECT
			person_id
				, condition_concept_id
			, dateadd( day,30,condition_end_date) 
			, 1 AS event_type
			, NULL
		FROM #cteConditionTarget
	) RAWDATA
) e
WHERE (2 * e.start_ordinal) - e.overall_ord = 0;


--------------------------------------------#cteConditionEnds
SELECT
        c.person_id
	, c.condition_concept_id
	, c.condition_start_date
	, MIN(e.end_date) AS era_end_date
into #cteConditionEnds FROM #cteConditionTarget c
JOIN #cteEndDates e ON c.person_id = e.person_id AND c.condition_concept_id = e.condition_concept_id AND e.end_date >= c.condition_start_date
GROUP BY
        c.condition_occurrence_id
	, c.person_id
	, c.condition_concept_id
	, c.condition_start_date;


/**************************************
 3. 2�ܰ�: condition_era�� ������ �Է�
***************************************/ 
INSERT INTO nhis_nsc_new.dbo.CONDITION_ERA_cpt4
 (person_id, condition_concept_id, condition_era_start_date, condition_era_end_date, condition_occurrence_count)
SELECT
	person_id
	, condition_concept_id
	, MIN(condition_start_date) AS condition_era_start_date
	, era_end_date AS condition_era_end_date
	, COUNT(*) AS condition_occurrence_count
FROM #cteConditionEnds
GROUP BY person_id, condition_concept_id, era_end_date
ORDER BY person_id, condition_concept_id