REGISTER '../target/monetdb-outputformat.jar';
raw = LOAD './supplier.tbl' USING PigStorage('|') AS (s_suppkey:int, s_name:chararray , s_address : chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);
STORE raw INTO './results/' USING nl.cwi.da.monetdb.loader.hadoop.MonetDBStoreFunc;
