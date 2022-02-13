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
  customer_count INT
);

-- vine table
CREATE TABLE vine_table (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);

select * from customers_table; 
select * from products_table;  
select * from review_id_table; 
select * from vine_table;

----------------------------------------------------------

-- records in originating table 
select count(*) from vine_table;

-- create table for total votes >= 20 
CREATE TABLE vine_table_twenty_and_over (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

-- validate creation
select * from vine_table_twenty_and_over;

-- insert total_votes >= 20: 
INSERT into vine_table_twenty_and_over 
select * from vine_table 
where total_votes >= 20;

-- create table where number of helpful_votes/total_votes >= .5 
CREATE TABLE vine_table_half_helpful (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

insert into vine_table_half_helpful
select * from vine_table_twenty_and_over
WHERE CAST(helpful_votes AS FLOAT)/CAST(total_votes AS FLOAT) >=0.5

-- query paid count of vine_table_half_helpful
select count(*) from vine_table_half_helpful

-- Create table having rows with where a review was written 
-- as part of the Vine program (paid), vine == 'Y'.
CREATE TABLE vine_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);

insert into vine_paid
select * from vine_table_half_helpful
WHERE vine = 'Y';

-- Create table having rows with where a review was written 
-- as part of the Vine program (paid), vine == 'N'.
CREATE TABLE vine_not_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);

insert into vine_not_paid
select * from vine_table_half_helpful
WHERE vine = 'N';

-- expected results: 
select count(*) from vine_table_half_helpful
where vine = 'Y' and star_rating = 5

select count(*) from vine_table_half_helpful
where vine = 'N' and star_rating = 5

-------------------------------------------------------------------
-- Create table having rows with where a review was written 
-- as part of the Vine program (paid), vine == 'Y'.
CREATE TABLE vine_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

insert into vine_table
select * from vine_table_half_helpful
WHERE vine = 'Y';

-- Create table having rows with where a review was written 
-- as part of the Vine program (paid), vine == 'N'.
CREATE TABLE vine_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

insert into vine_table
select * from vine_table_half_helpful
WHERE vine = 'N';


-----------------------------------------------------





-- Create table having rows with where a review was written 
-- as part of the Vine program (paid), vine == 'N'.
CREATE TABLE vine_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

insert into vine_table
select * from vine_table_half_helpful
WHERE vine = 'N';

select count(*) from vine_table;


# Create table having all rows where a review was written as part of the Vine program (paid), vine == 'N'.
CREATE TABLE vine_not_paid (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
)

insert into vine_table
select * from vine_table_half_helpful
WHERE vine = 'N';

select count(*) from vine_paid;
select count(*) from vine_not_paid;

COPY vine_table TO 'C:\Users\kkubi\Class Repo\Amazon_Vine_Analysis\vine_table.csv' DELIMITER ',' CSV HEADER;



