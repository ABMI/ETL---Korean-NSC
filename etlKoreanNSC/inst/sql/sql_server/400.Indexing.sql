/*********************************************************************************
# Copyright 2014 Observational Health Data Sciences and Informatics
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/

/************************

 ####### #     # ####### ######      #####  ######  #     #           #######      #####     ###
 #     # ##   ## #     # #     #    #     # #     # ##   ##    #    # #           #     #     #  #    # #####  ###### #    # ######  ####
 #     # # # # # #     # #     #    #       #     # # # # #    #    # #                 #     #  ##   # #    # #       #  #  #      #
 #     # #  #  # #     # ######     #       #     # #  #  #    #    # ######       #####      #  # #  # #    # #####    ##   #####   ####
 #     # #     # #     # #          #       #     # #     #    #    #       # ###       #     #  #  # # #    # #        ##   #           #
 #     # #     # #     # #          #     # #     # #     #     #  #  #     # ### #     #     #  #   ## #    # #       #  #  #      #    #
 ####### #     # ####### #           #####  ######  #     #      ##    #####  ###  #####     ### #    # #####  ###### #    # ######  ####


sql server script to create the required indexes within OMOP common data model, version 5.3

last revised: 14-November-2017

author:  Patrick Ryan, Clair Blacketer

description:  These primary keys and indices are considered a minimal requirement to ensure adequate performance of analyses.

*************************/


/************************
*************************
*************************
*************************

Primary key constraints

*************************
*************************
*************************
************************/

--use @NHISNSC_database

/************************

Standardized vocabulary

************************/
/**
Use @NHISNSC_database

ALTER TABLE  @NHISNSC_database.concept ADD CONSTRAINT xpk_concept PRIMARY KEY NONCLUSTERED (concept_id);

ALTER TABLE  @NHISNSC_database.vocabulary ADD CONSTRAINT xpk_vocabulary PRIMARY KEY NONCLUSTERED (vocabulary_id);

ALTER TABLE  @NHISNSC_database.domain ADD CONSTRAINT xpk_domain PRIMARY KEY NONCLUSTERED (domain_id);

ALTER TABLE  @NHISNSC_database.concept_class ADD CONSTRAINT xpk_concept_class PRIMARY KEY NONCLUSTERED (concept_class_id);

ALTER TABLE  @NHISNSC_database.concept_relationship ADD CONSTRAINT xpk_concept_relationship PRIMARY KEY NONCLUSTERED (concept_id_1,concept_id_2,relationship_id);

ALTER TABLE  @NHISNSC_database.relationship ADD CONSTRAINT xpk_relationship PRIMARY KEY NONCLUSTERED (relationship_id);

ALTER TABLE  @NHISNSC_database.concept_ancestor ADD CONSTRAINT xpk_concept_ancestor PRIMARY KEY NONCLUSTERED (ancestor_concept_id,descendant_concept_id);

ALTER TABLE  @NHISNSC_database.source_to_concept_map ADD CONSTRAINT xpk_source_to_concept_map PRIMARY KEY NONCLUSTERED (source_vocabulary_id,target_concept_id,source_code,valid_end_date);

ALTER TABLE  @NHISNSC_database.drug_strength ADD CONSTRAINT xpk_drug_strength PRIMARY KEY NONCLUSTERED (drug_concept_id, ingredient_concept_id);

ALTER TABLE  @NHISNSC_database.cohort_definition ADD CONSTRAINT xpk_cohort_definition PRIMARY KEY NONCLUSTERED (cohort_definition_id);

ALTER TABLE  @NHISNSC_database.attribute_definition ADD CONSTRAINT xpk_attribute_definition PRIMARY KEY NONCLUSTERED (attribute_definition_id);
**/

/**************************

Standardized meta-data

***************************/



/************************

Standardized clinical data

************************/

/**PRIMARY KEY NONCLUSTERED constraints**/

ALTER TABLE  @NHISNSC_database.person ADD CONSTRAINT xpk_person PRIMARY KEY NONCLUSTERED ( person_id ) ;

ALTER TABLE  @NHISNSC_database.observation_period ADD CONSTRAINT xpk_observation_period PRIMARY KEY NONCLUSTERED ( observation_period_id ) ;

ALTER TABLE  @NHISNSC_database.specimen ADD CONSTRAINT xpk_specimen PRIMARY KEY NONCLUSTERED ( specimen_id ) ;

ALTER TABLE  @NHISNSC_database.death ADD CONSTRAINT xpk_death PRIMARY KEY NONCLUSTERED ( person_id ) ;

ALTER TABLE  @NHISNSC_database.visit_occurrence ADD CONSTRAINT xpk_visit_occurrence PRIMARY KEY NONCLUSTERED ( visit_occurrence_id ) ;

ALTER TABLE  @NHISNSC_database.visit_detail ADD CONSTRAINT xpk_visit_detail PRIMARY KEY NONCLUSTERED ( visit_detail_id ) ;

ALTER TABLE  @NHISNSC_database.procedure_occurrence ADD CONSTRAINT xpk_procedure_occurrence PRIMARY KEY NONCLUSTERED ( procedure_occurrence_id ) ;

ALTER TABLE  @NHISNSC_database.drug_exposure ADD CONSTRAINT xpk_drug_exposure PRIMARY KEY NONCLUSTERED ( drug_exposure_id ) ;

ALTER TABLE  @NHISNSC_database.device_exposure ADD CONSTRAINT xpk_device_exposure PRIMARY KEY NONCLUSTERED ( device_exposure_id ) ;

ALTER TABLE  @NHISNSC_database.condition_occurrence ADD CONSTRAINT xpk_condition_occurrence PRIMARY KEY NONCLUSTERED ( condition_occurrence_id ) ;

ALTER TABLE  @NHISNSC_database.measurement ADD CONSTRAINT xpk_measurement PRIMARY KEY NONCLUSTERED ( measurement_id ) ;

ALTER TABLE  @NHISNSC_database.note ADD CONSTRAINT xpk_note PRIMARY KEY NONCLUSTERED ( note_id ) ;

ALTER TABLE  @NHISNSC_database.note_nlp ADD CONSTRAINT xpk_note_nlp PRIMARY KEY NONCLUSTERED ( note_nlp_id ) ;

ALTER TABLE  @NHISNSC_database.observation  ADD CONSTRAINT xpk_observation PRIMARY KEY NONCLUSTERED ( observation_id ) ;




/************************

Standardized health system data

************************/


ALTER TABLE  @NHISNSC_database.location ADD CONSTRAINT xpk_location PRIMARY KEY NONCLUSTERED ( location_id ) ;

ALTER TABLE  @NHISNSC_database.care_site ADD CONSTRAINT xpk_care_site PRIMARY KEY NONCLUSTERED ( care_site_id ) ;

ALTER TABLE  @NHISNSC_database.provider ADD CONSTRAINT xpk_provider PRIMARY KEY NONCLUSTERED ( provider_id ) ;



/************************

Standardized health economics

************************/


ALTER TABLE  @NHISNSC_database.payer_plan_period ADD CONSTRAINT xpk_payer_plan_period PRIMARY KEY NONCLUSTERED ( payer_plan_period_id ) ;

ALTER TABLE  @NHISNSC_database.cost ADD CONSTRAINT xpk_visit_cost PRIMARY KEY NONCLUSTERED ( cost_id ) ;


/************************

Standardized derived elements

************************/

ALTER TABLE  @NHISNSC_database.cohort ADD CONSTRAINT xpk_cohort PRIMARY KEY NONCLUSTERED ( cohort_definition_id, subject_id, cohort_start_date, cohort_end_date  ) ;

ALTER TABLE  @NHISNSC_database.cohort_attribute ADD CONSTRAINT xpk_cohort_attribute PRIMARY KEY NONCLUSTERED ( cohort_definition_id, subject_id, cohort_start_date, cohort_end_date, attribute_definition_id ) ;


ALTER TABLE  @NHISNSC_database.drug_era ADD CONSTRAINT xpk_drug_era PRIMARY KEY NONCLUSTERED ( drug_era_id ) ;

ALTER TABLE  @NHISNSC_database.dose_era  ADD CONSTRAINT xpk_dose_era PRIMARY KEY NONCLUSTERED ( dose_era_id ) ;

ALTER TABLE  @NHISNSC_database.condition_era ADD CONSTRAINT xpk_condition_era PRIMARY KEY NONCLUSTERED ( condition_era_id ) ;


/************************
*************************
*************************
*************************

Indices

*************************
*************************
*************************
************************/

/************************

Standardized vocabulary

************************/

CREATE UNIQUE CLUSTERED INDEX idx_concept_concept_id ON @NHISNSC_database.concept (concept_id ASC);
CREATE INDEX idx_concept_code ON @NHISNSC_database.concept (concept_code ASC);
CREATE INDEX idx_concept_vocabluary_id ON @NHISNSC_database.concept (vocabulary_id ASC);
CREATE INDEX idx_concept_domain_id ON @NHISNSC_database.concept (domain_id ASC);
CREATE INDEX idx_concept_class_id ON @NHISNSC_database.concept (concept_class_id ASC);

CREATE UNIQUE CLUSTERED INDEX idx_vocabulary_vocabulary_id ON @NHISNSC_database.vocabulary (vocabulary_id ASC);

CREATE UNIQUE CLUSTERED INDEX idx_domain_domain_id ON @NHISNSC_database.domain (domain_id ASC);

CREATE UNIQUE CLUSTERED INDEX idx_concept_class_class_id ON @NHISNSC_database.concept_class (concept_class_id ASC);

CREATE INDEX idx_concept_relationship_id_1 ON @NHISNSC_database.concept_relationship (concept_id_1 ASC);
CREATE INDEX idx_concept_relationship_id_2 ON @NHISNSC_database.concept_relationship (concept_id_2 ASC);
CREATE INDEX idx_concept_relationship_id_3 ON @NHISNSC_database.concept_relationship (relationship_id ASC);

CREATE UNIQUE CLUSTERED INDEX idx_relationship_rel_id ON @NHISNSC_database.relationship (relationship_id ASC);

CREATE CLUSTERED INDEX idx_concept_synonym_id ON @NHISNSC_database.concept_synonym (concept_id ASC);

CREATE CLUSTERED INDEX idx_concept_ancestor_id_1 ON @NHISNSC_database.concept_ancestor (ancestor_concept_id ASC);
CREATE INDEX idx_concept_ancestor_id_2 ON @NHISNSC_database.concept_ancestor (descendant_concept_id ASC);

CREATE CLUSTERED INDEX idx_source_to_concept_map_id_3 ON @NHISNSC_database.source_to_concept_map (target_concept_id ASC);
CREATE INDEX idx_source_to_concept_map_id_1 ON @NHISNSC_database.source_to_concept_map (source_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_id_2 ON @NHISNSC_database.source_to_concept_map (target_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_code ON @NHISNSC_database.source_to_concept_map (source_code ASC);

CREATE CLUSTERED INDEX idx_drug_strength_id_1 ON @NHISNSC_database.drug_strength (drug_concept_id ASC);
CREATE INDEX idx_drug_strength_id_2 ON @NHISNSC_database.drug_strength (ingredient_concept_id ASC);

CREATE CLUSTERED INDEX idx_cohort_definition_id ON @NHISNSC_database.cohort_definition (cohort_definition_id ASC);

CREATE CLUSTERED INDEX idx_attribute_definition_id ON @NHISNSC_database.attribute_definition (attribute_definition_id ASC);


/**************************

Standardized meta-data

***************************/





/************************

Standardized clinical data

************************/

CREATE UNIQUE CLUSTERED INDEX idx_person_id ON @NHISNSC_database.person (person_id ASC);

CREATE CLUSTERED INDEX idx_observation_period_id ON @NHISNSC_database.observation_period (person_id ASC);

CREATE CLUSTERED INDEX idx_specimen_person_id ON @NHISNSC_database.specimen (person_id ASC);
CREATE INDEX idx_specimen_concept_id ON @NHISNSC_database.specimen (specimen_concept_id ASC);

CREATE CLUSTERED INDEX idx_death_person_id ON @NHISNSC_database.death (person_id ASC);

CREATE CLUSTERED INDEX idx_visit_person_id ON @NHISNSC_database.visit_occurrence (person_id ASC);
CREATE INDEX idx_visit_concept_id ON @NHISNSC_database.visit_occurrence (visit_concept_id ASC);

CREATE CLUSTERED INDEX idx_visit_detail_person_id ON @NHISNSC_database.visit_detail (person_id ASC);
CREATE INDEX idx_visit_detail_concept_id ON @NHISNSC_database.visit_detail (visit_detail_concept_id ASC);

CREATE CLUSTERED INDEX idx_procedure_person_id ON @NHISNSC_database.procedure_occurrence (person_id ASC);
CREATE INDEX idx_procedure_concept_id ON @NHISNSC_database.procedure_occurrence (procedure_concept_id ASC);
CREATE INDEX idx_procedure_visit_id ON @NHISNSC_database.procedure_occurrence (visit_occurrence_id ASC);

CREATE CLUSTERED INDEX idx_drug_person_id ON @NHISNSC_database.drug_exposure (person_id ASC);
CREATE INDEX idx_drug_concept_id ON @NHISNSC_database.drug_exposure (drug_concept_id ASC);
CREATE INDEX idx_drug_visit_id ON @NHISNSC_database.drug_exposure (visit_occurrence_id ASC);

CREATE CLUSTERED INDEX idx_device_person_id ON @NHISNSC_database.device_exposure (person_id ASC);
CREATE INDEX idx_device_concept_id ON @NHISNSC_database.device_exposure (device_concept_id ASC);
CREATE INDEX idx_device_visit_id ON @NHISNSC_database.device_exposure (visit_occurrence_id ASC);

CREATE CLUSTERED INDEX idx_condition_person_id ON @NHISNSC_database.condition_occurrence (person_id ASC);
CREATE INDEX idx_condition_concept_id ON @NHISNSC_database.condition_occurrence (condition_concept_id ASC);
CREATE INDEX idx_condition_visit_id ON @NHISNSC_database.condition_occurrence (visit_occurrence_id ASC);

CREATE CLUSTERED INDEX idx_measurement_person_id ON @NHISNSC_database.measurement (person_id ASC);
CREATE INDEX idx_measurement_concept_id ON @NHISNSC_database.measurement (measurement_concept_id ASC);
CREATE INDEX idx_measurement_visit_id ON @NHISNSC_database.measurement (visit_occurrence_id ASC);

CREATE CLUSTERED INDEX idx_note_person_id ON @NHISNSC_database.note (person_id ASC);
CREATE INDEX idx_note_concept_id ON @NHISNSC_database.note (note_type_concept_id ASC);
CREATE INDEX idx_note_visit_id ON @NHISNSC_database.note (visit_occurrence_id ASC);

CREATE CLUSTERED INDEX idx_note_nlp_note_id ON @NHISNSC_database.note_nlp (note_id ASC);
CREATE INDEX idx_note_nlp_concept_id ON @NHISNSC_database.note_nlp (note_nlp_concept_id ASC);

CREATE CLUSTERED INDEX idx_observation_person_id ON @NHISNSC_database.observation (person_id ASC);
CREATE INDEX idx_observation_concept_id ON @NHISNSC_database.observation (observation_concept_id ASC);
CREATE INDEX idx_observation_visit_id ON @NHISNSC_database.observation (visit_occurrence_id ASC);

CREATE INDEX idx_fact_relationship_id_1 ON @NHISNSC_database.fact_relationship (domain_concept_id_1 ASC);
CREATE INDEX idx_fact_relationship_id_2 ON @NHISNSC_database.fact_relationship (domain_concept_id_2 ASC);
CREATE INDEX idx_fact_relationship_id_3 ON @NHISNSC_database.fact_relationship (relationship_concept_id ASC);



/************************

Standardized health system data

************************/





/************************

Standardized health economics

************************/

CREATE CLUSTERED INDEX idx_period_person_id ON @NHISNSC_database.payer_plan_period (person_id ASC);





/************************

Standardized derived elements

************************/


CREATE INDEX idx_cohort_subject_id ON @NHISNSC_database.cohort (subject_id ASC);
CREATE INDEX idx_cohort_c_definition_id ON @NHISNSC_database.cohort (cohort_definition_id ASC);

CREATE INDEX idx_ca_subject_id ON @NHISNSC_database.cohort_attribute (subject_id ASC);
CREATE INDEX idx_ca_definition_id ON @NHISNSC_database.cohort_attribute (cohort_definition_id ASC);

CREATE CLUSTERED INDEX idx_drug_era_person_id ON @NHISNSC_database.drug_era (person_id ASC);
CREATE INDEX idx_drug_era_concept_id ON @NHISNSC_database.drug_era (drug_concept_id ASC);

CREATE CLUSTERED INDEX idx_dose_era_person_id ON @NHISNSC_database.dose_era (person_id ASC);
CREATE INDEX idx_dose_era_concept_id ON @NHISNSC_database.dose_era (drug_concept_id ASC);

CREATE CLUSTERED INDEX idx_condition_era_person_id ON @NHISNSC_database.condition_era (person_id ASC);
CREATE INDEX idx_condition_era_concept_id ON @NHISNSC_database.condition_era (condition_concept_id ASC);


declare @db_name varchar(100) = concat(left('NSC_syc.dbo', CHARINDEX('.dbo', 'NSC_syc.dbo')-1), '_log');
dbcc shrinkfile (@db_name,10)