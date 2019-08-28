**ETL process for conversion of NHIS-Korean National Sample Cohort into OMOP-CDM version 5.3.1**
==============================================

# Description
* ETL code for converting Korean National Sample Cohort (NSC) derived from national insurance health service into OMOP-CDM v5.3.1 via SQL-server developed by Ajou University

## Encoding
* UTF-8

## Contributors
* Seongwon Lee, Seng Chan You, Jaehyeong Cho, Soo-Yeon Cho, Hojun Park, Sungjae Jung, Youngjin Choi, Rae Woong Park

## Information about NHIS-Korean National Sample Cohort
* Lee et al., “Cohort Profile: The National Health Insurance Service–National Sample Cohort (NHIS-NSC), South Korea.” International Journal of Epidemiology, January 28, 2016, dyv319. doi:10.1093/ije/dyv319.
https://academic.oup.com/ije/article-lookup/doi/10.1093/ije/dyv319


# How To Use

## R Installation
```{r}
install.packages("devtools")
devtools::install_github("ohdsi/ETL---Korean-NSC/etlKoreanNSC")
```

## Execution ETL 
```{r}
# fill out the connection details ---------------------------------------------------------------------------
connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = 'sql server'
    , server = Sys.getenv("myCdmServer")
    , schema = Sys.getenv("myCdmSchema")
    , user = Sys.getenv("userId")
    , password = Sys.getenv("password")
)
DatabaseConnector::connect(connectionDetails = connectionDetails)
connection <- DatabaseConnector::connect(connectionDetails)


# where should the logs go? --------------------------------------------------------------------------------
outputFolder <- "output"


# run the job ----------------------------------------------------------------------------------------------
etlKoreanNSC::executeNHISETL(NHISNSC_rawdata <- "nhisnsc2013original.dbo",
                           NHISNSC_database <- "NHIS_NSC_v5_3_1.dbo",
                           Mapping_database <- "NHIS_NSC_NEW_MAPPING.dbo",
                           NHIS_JK <- "NHID_JK",
                           NHIS_20T <- "NHID_GY20_T1",
                           NHIS_30T <- "NHID_GY30_T1",
                           NHIS_40T <- "NHID_GY40_T1",
                           NHIS_60T <- "NHID_GY60_T1",
                           NHIS_GJ <- "NHID_GJ",
                           NHIS_YK <- "NHID_YK",
                           
                           connection,
                           outputFolder,
                           
                           CDM_ddl = TRUE,
                           #import_voca = TRUE,        Importing voca could be unnecessary
                           cdm_source = TRUE,
                           master_table = TRUE,
                           location = TRUE,
                           care_site = TRUE,
                           person = TRUE,
                           death = TRUE,
                           observation_period = TRUE,
                           visit_occurrence = TRUE,
                           condition_occurrence = TRUE,
                           observation = TRUE,
                           drug_exposure = TRUE,
                           procedure_occurrence = TRUE,
                           device_exposure = TRUE,
                           measurement = TRUE,
                           payer_plan_period = TRUE,
                           cost = TRUE,
                           generateEra = TRUE,
                           dose_era = TRUE,
                           indexing = TRUE,
                           constraints = TRUE,
                           data_cleansing = TRUE)
```
