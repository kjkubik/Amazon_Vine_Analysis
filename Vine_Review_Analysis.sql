-- Amazon Reviews at https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt
-- Jewelry Review: https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Jewelry_v1_00.tsv.gz

-- Using AWS, a database was created and used in PostgreSQL (AKA pgAdmin).
-- ETL steps were completed in PySpark. ETL steps are in Amazon_Reviews_ETL.ipynb. 
-- The resulting tables were created and filled: 

CREATE TABLE review_id_table (
  review_id TEXT PRIMARY KEY NOT NULL,
  customer_id INTEGER,
  product_id TEXT,
  product_parent INTEGER,
  review_date DATE -- format is yyyy-mm-dd
);

-- products table
CREATE TABLE products_table (
  product_id TEXT PRIMARY KEY NOT NULL UNIQUE,
  product_title TEXT
);

-- customer table
CREATE TABLE customers_table (
  customer_id INT PRIMARY KEY NOT NULL UNIQUE,
  customer_COUNT INT
);

-- vine table (#4)
CREATE TABLE vine_table (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);

SELECT * FROM customers_table; 
SELECT * FROM products_table;  
SELECT * FROM review_id_table; 
SELECT * FROM vine_table; -- validating import (#5)


-- count records in originating vine table 
SELECT COUNT(*) FROM vine_table; --(#5) 

-- create table for total votes >= 20 (#1)
CREATE TABLE vine_table_twenty_and_over (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

-- validate creation (#1)
SELECT * FROM vine_table_twenty_and_over;

-- insert total_votes >= 20: 
INSERT INTO vine_table_twenty_and_over 
SELECT * FROM vine_table 
WHERE total_votes >= 20;

-- create table where number of helpful_votes/total_votes >= .5 (#2)
CREATE TABLE vine_table_half_helpful (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

-- insert helpful_votes/total_votes >= .5 (#2)
INSERT INTO vine_table_half_helpful
SELECT * FROM vine_table_twenty_and_over
WHERE CAST(helpful_votes AS FLOAT)/CAST(total_votes AS FLOAT) >=0.5

-- query paid COUNT of vine_table_half_helpful (#2)
SELECT COUNT(*) FROM vine_table_half_helpful

-- create table having rows with where a review was written 
-- as part of the Vine program (paid), vine == 'Y'. (#3)
CREATE TABLE vine_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);

-- insert  vine == 'Y'. (#3)
INSERT INTO vine_paid
SELECT * FROM vine_table_half_helpful
WHERE vine = 'Y';

-- Create table having rows with WHERE a review was written 
-- as part of the Vine program (paid), vine == 'N'. (#4)
CREATE TABLE vine_not_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);

-- insert  vine == 'Y'. (#4)
INSERT INTO vine_not_paid
SELECT * FROM vine_table_half_helpful
WHERE vine = 'N';


-- Create table containing results:
-- 1) total number of reviews (that are found 'helpful' and having more than 20 votes)
-- 2) 
-- 3) count records in vine_paid (out of five star reviews, how many are 'paid')
-- 4) count records in vine_not_paid (out of five star reviews, how many are 'not paid')
-- 5) what % are paid and what % are not paid
-- 5) % paid (count(paid)/count(all five stars))
-- 6) % not paid (count(not paid)/count(all five stars))

drop table vine_results; 

CREATE TABLE vine_results (
  review_id TEXT PRIMARY KEY,                  -- we are just going to put 'review results' HOWEVER this could be done better
  total_reviews FLOAT, 		                   -- total number of reviews ('helpful' and >= 20 votes)
  total_reviews_vine_paid FLOAT,               -- count total number of reviews ('helpful' and >= 20 votes and paid)
  total_reviews_vine_not_paid FLOAT,           -- count total number of reviews ('helpful' and >= 20 votes and NOT paid)
  percent_reviews_paid FLOAT,                  -- calculate total_reviews_vine_paid/total_reviews * 100
  percent_reviews_not_paid FLOAT,              -- calculate total_reviews_vine_not_paid/total_reviews * 100
  total_five_star_reviews FLOAT,               -- number of 5 * reviews (count of five start reviews)
  total_five_star_reviews_vine_paid FLOAT,     -- count total number of five star reviews ('helpful' and >= 20 votes and paid   
  total_five_star_reviews_vine_not_paid FLOAT, -- count total number of five star reviews ('helpful' and >= 20 votes and NOT paid   
  percent_paid_five_star FLOAT,                -- calculate total_five_star_reviews_vine_paid/totalfive_star_reviews * 100
  percent_not_paid_five_star FLOAT             -- calculate total_five_star_reviews_vine_not_paid/total_five_star_reviews * 100 
);

-- initialized table
INSERT INTO vine_results (review_id,total_reviews,total_five_star_reviews,total_reviews_vine_paid,total_reviews_vine_not_paid,
total_five_star_reviews_vine_paid,total_five_star_reviews_vine_not_paid,percent_reviews_paid,
percent_reviews_not_paid,percent_paid_five_star,percent_not_paid_five_star)
VALUES ('review results',0,0,0,0,0,0,0,0,0,0);

UPDATE vine_results 
SET total_reviews = (SELECT COUNT(*) from vine_table_half_helpful)
where review_id = 'review results';

UPDATE vine_results 
SET total_five_star_reviews = (SELECT COUNT(*) from vine_table_half_helpful where star_rating = 5)
where review_id = 'review results';

UPDATE vine_results 
SET total_reviews_vine_paid = (SELECT COUNT(*) from vine_table_half_helpful where vine = 'Y')
where review_id = 'review results';

UPDATE vine_results 
SET total_reviews_vine_not_paid = (SELECT COUNT(*) from vine_table_half_helpful where vine = 'N')
where review_id = 'review results';

UPDATE vine_results 
SET total_five_star_reviews_vine_paid = (SELECT COUNT(*) from vine_table_half_helpful where star_rating = 5 and vine = 'Y')
where review_id = 'review results';

UPDATE vine_results 
SET total_five_star_reviews_vine_not_paid = (SELECT COUNT(*) from vine_table_half_helpful where star_rating = 5 and vine = 'N')
where review_id = 'review results';

UPDATE vine_results 
SET percent_reviews_paid = (SELECT ((total_reviews_vine_paid/total_reviews) * 100) FROM vine_results 
					WHERE review_id = 'review results');

UPDATE vine_results 
SET percent_reviews_not_paid = (SELECT ((total_reviews_vine_not_paid/total_reviews) * 100) FROM vine_results 
					WHERE review_id = 'review results');

UPDATE vine_results 
SET percent_paid_five_star = (SELECT ((total_five_star_reviews_vine_paid/total_five_star_reviews) * 100) FROM vine_results 
					WHERE review_id = 'review results');

UPDATE vine_results 
SET percent_not_paid_five_star = (SELECT ((total_five_star_reviews_vine_not_paid/total_five_star_reviews) * 100) FROM vine_results 
					WHERE review_id = 'review results');

select * from vine_results;