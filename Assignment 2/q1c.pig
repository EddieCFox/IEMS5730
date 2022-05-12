D = LOAD 'united_books' USING PigStorage('\t') AS (bigram:chararray, year:int, match_count:float, volume_count:int);
Bigram_Grouped = GROUP D BY bigram;
Bigram_Averaged = FOREACH Bigram_Grouped GENERATE group as bigram, AVG(D.match_count) as average;
STORE Bigram_Averaged INTO '/user/s1155160788/output' USING PigStorage('\t');