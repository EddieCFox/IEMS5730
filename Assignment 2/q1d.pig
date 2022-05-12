D = LOAD 'PartC' USING PigStorage('\t') AS (bigram:chararray, average:float);
D_Ordered = ORDER D BY average DESC;
Top20 = LIMIT D_Ordered 20;
STORE Top20 INTO '/user/s1155160788/output' USING PigStorage('\t');