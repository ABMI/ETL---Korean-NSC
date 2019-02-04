
etlExecute<-function(connectionDetails,
                     rawDatabaseSchema,
                     cdmDatabaseSchema,
                     tableJk,
                     tableGj,
                     table20,
                     table30,
                     table40,
                     table60,
                     cdmVer = "v5.3.1",
                     sourceToConceptVer="20181231", #Need to be checked
                     omopVocabularyVer = "v5.0 04-DEC-18"){

	pathToSql <- system.file("sql", "sql server", package = "etlKoreanNSC")
	if(createCdmDdl) {
		sql <- SqlRender::readSql(file.path(pathToSql),"000.OMOP CDM sql server ddl.sql")
		sql <- SqlRender::renderSql(sql,
									NHISNSC_rawdata=rawDatabaseSchema,
                     				NHISNSC_database=cdmDatabaseSchema,
				                    NHIS_JK=tableJk,
				                    NHIS_GJ=tableGj,
				                    NHIS_20T=table20,
				                    NHIS_30T=table30,
				                    NHIS_40T=table40,
				                    NHIS_60T=table60)$sql
	  	sql <- SqlRender::translateSql(sql, targetDialect = attr(connection, "dbms"))$sql
	    DatabaseConnector::executeSql(connection, sql)
	}
}



