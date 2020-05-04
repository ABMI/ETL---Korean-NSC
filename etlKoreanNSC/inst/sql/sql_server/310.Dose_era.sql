/**************************************
 --encoding : UTF-8
 --Author: OHDSI
  
@NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
@NHISNSC_database : DB for NHIS-NSC in CDM format
 
 --Description: SQL query for dose_era table from OHDSI
 --Generating Table: DOSE_ERA
***************************************/

/**************************************
 1. Create dose_era table
***************************************/ 
/*
 CREATE TABLE @NHISNSC_database.DOSE_ERA (
     dose_era_id					INTEGER	 identity(1,1)    NOT NULL , 
     person_id						INTEGER     NOT NULL ,
     drug_concept_id				INTEGER   NOT NULL ,
     unit_concept_id				INTEGER      NOT NULL ,
     dose_value						float  NOT NULL ,
     dose_era_start_date			DATE 		NOT	NULL, 
	 dose_era_end_date				DATE 		NOT	NULL
);
*/

/**************************************
 2. Step 1, Check the required data
***************************************/ 
--------------------------------------------@Mapping_database.cteDrugTarget
IF OBJECT_ID('@Mapping_database.cteDrugTarget', 'U') IS NOT NULL
	DROP TABLE @Mapping_database.cteDrugTarget;
IF OBJECT_ID('@Mapping_database.cteEndDates', 'U') IS NOT NULL
	DROP TABLE @Mapping_database.cteEndDates;
IF OBJECT_ID('@Mapping_database.cteDoseEraEnds', 'U') IS NOT NULL
	DROP TABLE @Mapping_database.cteDoseEraEnds;

SELECT
	d.drug_exposure_id
	, d.person_id
	, c.concept_id AS ingredient_concept_id
	, NULL AS unit_concept_id
	, d.quantity AS dose_value
	, d.drug_exposure_start_date
	, d.days_supply AS days_supply
	, COALESCE(d.drug_exposure_end_date, DATEADD(DAY, d.days_supply, d.drug_exposure_start_date), DATEADD(DAY, 1, drug_exposure_start_date)) AS drug_exposure_end_date
INTO @Mapping_database.cteDrugTarget 
FROM @NHISNSC_database.DRUG_EXPOSURE d
	 JOIN @Mapping_database.CONCEPT_ANCESTOR ca ON ca.descendant_concept_id = d.drug_concept_id
	 JOIN @Mapping_database.CONCEPT c ON ca.ancestor_concept_id = c.concept_id
	 WHERE c.vocabulary_id in ('RxNorm', 'RxNorm Extension') 
	 AND c.concept_class_ID = 'Ingredient';
	
	
--------------------------------------------@Mapping_database.cteEndDates
SELECT
	person_id
	, ingredient_concept_id
	, unit_concept_id
	, dose_value
	, DATEADD( DAY, -30, event_date) AS end_date
INTO @Mapping_database.cteEndDates FROM
(
	SELECT
		person_id
		, ingredient_concept_id
		, unit_concept_id
		, dose_value
		, event_date
		, event_type
		, MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal
		, ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY event_date, event_type) AS overall_ord
	FROM
	(
		SELECT
			person_id
			, ingredient_concept_id
			, unit_concept_id
			, dose_value
			, drug_exposure_start_date AS event_date
			, -1 AS event_type, ROW_NUMBER() OVER(PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY drug_exposure_start_date) AS start_ordinal
		FROM @Mapping_database.cteDrugTarget 

		UNION ALL

		SELECT
			person_id
			, ingredient_concept_id
			, unit_concept_id
			, dose_value
			, DATEADD(DAY, 30, drug_exposure_end_date) AS drug_exposure_end_date
			, 1 AS event_type
			, NULL
		FROM @Mapping_database.cteDrugTarget
	) RAWDATA
) e
WHERE (2 * e.start_ordinal) - e.overall_ord = 0;

--------------------------------------------@Mapping_database.cteDoseEraEnds
SELECT
	dt.person_id
	, dt.ingredient_concept_id as drug_concept_id
	, dt.unit_concept_id 
	, dt.dose_value
	, dt.drug_exposure_start_date
	, MIN(e.end_date) AS dose_era_end_date
into @Mapping_database.cteDoseEraEnds FROM @Mapping_database.cteDrugTarget dt
JOIN @Mapping_database.cteEndDates e
ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
--AND dt.unit_concept_id = e.unit_concept_id AND dt.dose_value = e.dose_value		--Both unit_concpet_id and dose_value are excluded in both tables because of NULLs
GROUP BY
	dt.drug_exposure_id
	, dt.person_id
	, dt.ingredient_concept_id
	, dt.unit_concept_id
	, dt.dose_value
	, dt.drug_exposure_start_date;
	
	
/**************************************
 3. Step 2: Insert data into dose_era table
***************************************/ 
INSERT INTO @NHISNSC_database.dose_era (person_id, drug_concept_id, dose_value, dose_era_start_date, dose_era_end_date)
SELECT
	person_id
	, drug_concept_id
	, dose_value
	, MIN(drug_exposure_start_date) AS dose_era_start_date
	, dose_era_end_date
	from @Mapping_database.cteDoseEraEnds
GROUP BY person_id, drug_concept_id, unit_concept_id, dose_value, dose_era_end_date
ORDER BY person_id, drug_concept_id;

drop table @Mapping_database.cteDrugTarget, @Mapping_database.cteEndDates, @Mapping_database.cteDoseEraEnds;


dbcc shrinkfile (@NHISNSC_database_use,10)