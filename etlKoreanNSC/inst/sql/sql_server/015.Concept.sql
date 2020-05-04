insert into NSC_syc.dbo.concept select * from NHIS_NSC_2019.dbo.concept

declare @db_name varchar(100) = concat(left('@NHISNSC_database', CHARINDEX('.dbo', '@NHISNSC_database')-1), '_log');
dbcc shrinkfile (@db_name,10)