A = LOAD 'googlebooks-eng-all-1gram-20120701-a' USING PigStorage('\t') AS (bigram:chararray, year:int, match_count:int, volume_count:int);
B = LOAD 'googlebooks-eng-all-1gram-20120701-b' USING PigStorage('\t') AS (bigram:chararray, year:int, match_count:int, volume_count:int);
C = UNION A, B;
STORE C INTO '/user/s1155160788/output' USING PigStorage('\t');