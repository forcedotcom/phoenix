A = load 'examples/pig/testdata' as (a:chararray, b:chararray, c:int, d:chararray, e: datetime) ;
STORE A into 'hbase://TESTPHX2' using com.salesforce.phoenix.pig.PhoenixHBaseStorage('localhost','-batchSize 10');
--dump A;