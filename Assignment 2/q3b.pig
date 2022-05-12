movielens = LOAD 'movielens_large_updated.csv' USING PigStorage(',') AS (userId:int, movieId:int);
movielens_grpd = GROUP movielens BY movieId;
movielens_grpd_dbl = FOREACH movielens_grpd GENERATE group, movielens.userId AS userId1, movielens.userId AS userId2;
cowatch = FOREACH movielens_grpd_dbl GENERATE FLATTEN(userId1) as userId1, FLATTEN(userId2) as userId2;
cowatch_filtered = FILTER cowatch BY userId1 < userId2;
cowatch_grouped = GROUP cowatch_filtered BY (userId1, userId2);
cowatch_pairs = FOREACH cowatch_grouped GENERATE group, COUNT(cowatch_filtered.userId1) AS group_count;
individual_count_grouped = GROUP movielens BY userId;
individual_count = FOREACH individual_count_grouped GENERATE FLATTEN(group) AS userId, COUNT(movielens.movieId) AS ind_count;
cowatch_flattened = FOREACH cowatch_pairs GENERATE FLATTEN(group) AS (userId1, userId2), group_count;
cowatch_joined_beginner = JOIN cowatch_flattened BY userId1, individual_count BY userId;
cowatch_joined_intermediate = JOIN cowatch_joined_beginner BY userId2, individual_count BY userId;
cowatch_joined = FOREACH cowatch_joined_intermediate GENERATE $0 AS userId1, $1 AS userId2, $2 AS group_count, $4 AS user1Count, $6 AS user2Count;
cowatch_similarity_first = FOREACH cowatch_joined GENERATE userId1, userId2, ((float)(group_count)/((float)user1Count+(float)user2Count-(float)group_count)) AS similarity:float;
cowatch_similarity_second = FOREACH cowatch_joined GENERATE userId2, userId1, ((float)(group_count)/((float)user1Count+(float)user2Count-(float)group_count)) AS similarity:float;
similarity_union = UNION cowatch_similarity_first, cowatch_similarity_second;
similarity_grouped = GROUP similarity_union BY userId1;
top3 = FOREACH similarity_grouped {
    ordered = ORDER similarity_union BY similarity DESC;
    tops = LIMIT ordered 3;
    GENERATE group, tops.userId2;
    }
STORE top3 INTO '/user/s1155160788/output' USING PigStorage(',');