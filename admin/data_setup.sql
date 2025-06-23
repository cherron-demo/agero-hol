-------------------------Forecast Setup-------------------------

-- Using accountadmin is often suggested for quickstarts, but any role with sufficient privledges can work
USE ROLE ACCOUNTADMIN;

-- Create development database, schema for our work: 
CREATE OR REPLACE DATABASE agero_hol_db;
CREATE OR REPLACE SCHEMA ml_functions;

-- Use appropriate resources: 
USE DATABASE agero_hol_db;
USE SCHEMA ml_functions;

-- Create warehouse to work with: 
CREATE OR REPLACE WAREHOUSE agero_hol_wh;
USE WAREHOUSE agero_hol_wh;

-- Create a csv file format to be used to ingest from the stage: 
CREATE OR REPLACE FILE FORMAT agero_hol_db.ml_functions.csv_ff
    TYPE = 'csv'
    SKIP_HEADER = 1,
    COMPRESSION = AUTO;

-- Create an external stage pointing to AWS S3 for loading our data:
CREATE OR REPLACE STAGE s3load 
    COMMENT = 'Quickstart S3 Stage Connection'
    URL = 's3://sfquickstarts/hol_snowflake_cortex_ml_for_sql/'
    FILE_FORMAT = agero_hol_db.ml_functions.csv_ff;

-- Define our table schema
CREATE OR REPLACE TABLE agero_hol_db.ml_functions.bank_marketing(
    CUSTOMER_ID TEXT,
    AGE NUMBER,
    JOB TEXT, 
    MARITAL TEXT, 
    EDUCATION TEXT, 
    DEFAULT TEXT, 
    HOUSING TEXT, 
    LOAN TEXT, 
    CONTACT TEXT, 
    MONTH TEXT, 
    DAY_OF_WEEK TEXT, 
    DURATION NUMBER(4, 0), 
    CAMPAIGN NUMBER(2, 0), 
    PDAYS NUMBER(3, 0), 
    PREVIOUS NUMBER(1, 0), 
    POUTCOME TEXT, 
    EMPLOYEE_VARIATION_RATE NUMBER(2, 1), 
    CONSUMER_PRICE_INDEX NUMBER(5, 3), 
    CONSUMER_CONFIDENCE_INDEX NUMBER(3,1), 
    EURIBOR_3_MONTH_RATE NUMBER(4, 3),
    NUMBER_EMPLOYEES NUMBER(5, 1),
    CLIENT_SUBSCRIBED BOOLEAN,
    TIMESTAMP TIMESTAMP_NTZ(9)
);

-- Ingest data from S3 into our table:
COPY INTO agero_hol_db.ml_functions.bank_marketing
FROM @s3load/customers.csv;

-- View a sample of the ingested data: 
SELECT * FROM agero_hol_db.ml_functions.bank_marketing LIMIT 100;


-------------------------Call Transcript Setup-------------------------

CREATE OR REPLACE SCHEMA agero_hol_db.call_transcripts;

USE agero_hol_db.call_transcripts; 

CREATE or REPLACE file format agero_hol_db.call_transcripts.csvformat
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  type = 'CSV';

CREATE or REPLACE stage agero_hol_db.call_transcripts.call_transcripts_data_stage
  file_format = csvformat
  url = 's3://sfquickstarts/misc/call_transcripts/';

CREATE or REPLACE table agero_hol_db.call_transcripts.CALL_TRANSCRIPTS ( 
  date_created date,
  language varchar(60),
  country varchar(60),
  product varchar(60),
  category varchar(60),
  damage_type varchar(90),
  transcript varchar
);

COPY INTO agero_hol_db.call_transcripts.CALL_TRANSCRIPTS
FROM @call_transcripts_data_stage;

SELECT * FROM agero_hol_db.call_transcripts.CALL_TRANSCRIPTS;


-------------------------Image Classification Setup-------------------------

CREATE OR REPLACE SCHEMA agero_hol_db.image_classification;

USE agero_hol_db.image_classification; 

CREATE OR REPLACE STAGE image_stage
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

--upload images into this stage from the git rep admin/images


