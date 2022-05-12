CREATE TABLE A_Books (
            bigram STRING,
            year INT,
            match_count INT,
            volume_count INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as textfile;

CREATE TABLE B_Books (
            bigram STRING,
            year INT,
            match_count INT,
            volume_count INT)
row format DELIMITED FIELDS TERMINATED BY '\t'
stored as textfile;

LOAD DATA INPATH '/user/s1155160788/googlebooks-eng-all-1gram-20120701-a' OVERWRITE INTO TABLE A_Books;
LOAD DATA INPATH '/user/s1155160788/googlebooks-eng-all-1gram-20120701-b' OVERWRITE INTO TABLE B_Books;

CREATE TABLE Averages (
            bigram STRING,
            average INT)
row format DELIMITED FIELDS TERMINATED BY '\t'
stored as textfile;

INSERT OVERWRITE TABLE Averages SELECT bigram, avg(match_count) as Average FROM(
    SELECT bigram, match_count FROM A_Books
    UNION ALL
    SELECT bigram, match_count FROM B_Books
) United
GROUP BY bigram
ORDER BY Average DESC
LIMIT 20;

INSERT OVERWRITE directory '/user/s1155160788/output'
row format DELIMITED FIELDS TERMINATED BY '\t'
SELECT * from Averages