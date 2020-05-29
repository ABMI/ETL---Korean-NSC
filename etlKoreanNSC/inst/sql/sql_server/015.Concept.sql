insert into @NHISNSC_database.concept select * from AUSOMv5_3_1.dbo.CONCEPT

declare @log_file varchar(100) =  concat('@NHISNSC_database_use', '_log')
dbcc shrinkfile (@log_file,10)