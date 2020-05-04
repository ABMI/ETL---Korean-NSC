insert into @NHISNSC_database.concept select * from NHIS_NSC_2019.dbo.concept

dbcc shrinkfile (@NHISNSC_database_use,10)