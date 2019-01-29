/**************************************
 --encoding : UTF-8
 --Author: �̼���, ������
 --Date: 2018.08.21
 
 @NHISNSC_raw : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 --Description: ǥ����ȣƮDB T1 ���̺�� �� 30T, 40T, 60T, ����, �ڰ� ���̺��� primary key�� �����ϰ� ����ũ�� �Ϸù�ȣ�� ������ ���̺� ����
			   ������ �Ϸù�ȣ�� condition, drug, procedure, device ���̺��� primary key�� ���Ǹ�, ���� ���̺� ���� ������ �Ϸù�ȣ�� visit_occurrence�� �ԷµǴ� �������� primary key�� ���
			   , �ڰ� ���̺� ���� ������ �Ϸù�ȣ�� observation�� �ԷµǴ� primary key�� ���
               ��ȯ�� CDM �����Ϳ��� ǥ����ȣƮDB �����͸� �����ϱ� ���� �������� ������
 --Generating Table: SEQ_MASTER
***************************************/

/**************************************
 1. ���̺� ����
    : �Ϸù�ȣ(PK), �ҽ� ���̺�, person_id, 30T, 40T, 60T, ����, �ڰ� ���̺��� Primary key���� �÷����� �ϴ� ���̺� ����
***************************************/  
CREATE TABLE @NHISNSC_database.SEQ_MASTER (
	master_seq		BIGINT	identity(1, 1) PRIMARY KEY,
	source_table	CHAR(3)	NOT NULL, -- 30T, 40T, 60T�� 130, 140, 160. ������ 'GJT', �ڰ��� 'JKT'
	person_id		INT	NOT NULL, -- ���
	key_seq			BIGINT	NULL, -- 30T, 40T, 60T
	seq_no			NUMERIC(4)	NULL, -- 30T, 40T, 60T
	hchk_year		CHAR(4)	NULL, -- ����	
	stnd_y			CHAR(4) NULL, -- �ڰ�		--hchk_year �� �־ �ɵ�
)
-- 607738697

/**************************************
 2. 30T�� ���� ������ �Է�
    : �Ϸù�ȣ�� 3000000001, 30������ ����
***************************************/
-- 1) �Ϸù�ȣ �ʱ�ȭ
DBCC CHECKIDENT('@NHISNSC_database.seq_master', RESEED, 3000000000);

-- 2) ������ �Է�	576969959  36:35
INSERT INTO @NHISNSC_database.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '130', b.person_id, a.key_seq, a.seq_no
FROM @NHISNSC_rawdata.@NHIS_30T a, @NHISNSC_rawdata.@NHIS_20T b
WHERE a.key_seq=b.key_seq
;

/**************************************
 3. 40T�� ���� ������ �Է�
    : �Ϸù�ȣ�� 4000000001, 40������ ����
***************************************/
-- 1) �Ϸù�ȣ �ʱ�ȭ
DBCC CHECKIDENT('@NHISNSC_database.seq_master', RESEED, 4000000000);

-- 2) ������ �Է�	299379695	23:40
INSERT INTO @NHISNSC_database.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '140', b.person_id, a.key_seq, a.seq_no
FROM @NHISNSC_rawdata.@NHIS_40T a, @NHISNSC_rawdata.@NHIS_20T b
WHERE a.key_seq=b.key_seq
;

/**************************************
 4. 60T�� ���� ������ �Է�
    : �Ϸù�ȣ�� 6000000001, 60������ ����
***************************************/
-- 1) �Ϸù�ȣ �ʱ�ȭ  
DBCC CHECKIDENT('@NHISNSC_database.seq_master', RESEED, 6000000000);

-- 2) ������ �Է�	396777913	36:59
INSERT INTO @NHISNSC_database.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '160', b.person_id, a.key_seq, a.seq_no
FROM @NHISNSC_rawdata.@NHIS_60T a, @NHISNSC_rawdata.@NHIS_20T b
WHERE a.key_seq=b.key_seq
;

/**************************************
 5. ������ ���� ������ �Է�
    : �Ϸù�ȣ�� 800000000001, 8000������ ����
	: visit_occurrence_id�� 12�ڸ� �����̹Ƿ� �ڸ����� ���� ��
***************************************/
-- 1) �Ϸù�ȣ �ʱ�ȭ
DBCC CHECKIDENT('@NHISNSC_database.seq_master', RESEED, 800000000000);

-- 2) ������ �Է�	2210067		9
INSERT INTO @NHISNSC_database.SEQ_MASTER
	(source_table, person_id, hchk_year)
SELECT 'GJT', person_id, hchk_year
FROM @NHISNSC_rawdata.@NHIS_GJ
GROUP BY hchk_year, person_id
;
/**************************************
 6. �ڰݿ� ���� ������ �Է�
	: �Ϸù�ȣ�� 900000000001, 9000������ ����
**************************************/
-- 1) �Ϸù�ȣ �ʱ�ȭ
DBCC CHECKIDENT('@NHISNSC_database.seq_master', RESEED, 900000000000);

-- 2) ������ �Է�	12132633		1:15
INSERT INTO @NHISNSC_database.SEQ_MASTER
	(source_table, person_id, stnd_y)
SELECT 'JKT', person_id, STND_Y
FROM @NHISNSC_rawdata.dbo.@NHIS_JK
GROUP BY STND_Y, person_id;
;

/**************************************
 7. �Ϸù�ȣ �ڵ����� ��Ȱ��ȭ��Ŵ
***************************************/
DBCC CHECKIDENT('@NHISNSC_database.seq_master', NORESEED);
