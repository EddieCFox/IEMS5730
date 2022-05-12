A = LOAD 'googlebooks-eng-all-1gram-20120701-a' USING PigStorage('\t') AS (bigram:chararray, year:int, match_count:int, volume_count:int);
B = LOAD 'googlebooks-eng-all-1gram-20120701-b' USING PigStorage('\t') AS (bigram:chararray, year:int, match_count:int, volume_count:int);
C = UNION A, B;
Bigram_Grouped = GROUP C BY bigram;
Bigram_Averaged = FOREACH Bigram_Grouped GENERATE group as bigram, AVG(C.$2) as average;
Bigram_Ordered = ORDER Bigram_Averaged BY average DESC;
Top20 = LIMIT Bigram_Ordered 20;
STORE Top20 INTO '/user/s1155160788/output' USING PigStorage('\t');