﻿/**************************************
 --encoding : UTF-8
 --Author: 박지명
 --Date: 2019.02.04
 
@NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
@NHISNSC_database: DB for NHIS-NSC in CDM format

 --Description: CDM 버전과 RAW 데이터에 대한 기본 정보
 --Generating Table: CDM_SOURCE
***************************************/
Insert into @NHISNSC_database.CDM_SOURCE
			(cdm_source_name, cdm_source_abbreviation, cdm_holder, source_description, source_document_reference, cdm_etl_reference,
			 cdm_release_date, cdm_version, vocabulary_version)
values('The National Health Insurance Service–National Sample Cohort', 'NHIS-NSC', 'The National Health Insurance Service in South Korea',
		'A representative sample cohort of 1,025,340 participants was randomly selected, comprising 2.2% of the total eligible Korean population in 2002, and followed for 11 years until 2013 unless participants’ eligibility was disqualified due to death or emigration.',
		'http://nhiss.nhis.or.kr/bd/ab/bdaba021eng.do', 'https://github.com/OHDSI/ETL---Korean-NSC', 'Jul 20, 2017', 'v5.3.1', 'v5.0 04 Dec 18')