------Overall-------

--Make sure everyone has a Snowflake login
--give each attendee an ID (e.g. 01, 02, 03, etc)

create or replace role AGERO_HOL_ROLE; 
grant role AGERO_HOL_ROLE to role accountadmin;

--this needs to be per attendee so each person has an individual warehouse
CREATE OR REPLACE WAREHOUSE <WH_XX> WITH WAREHOUSE_SIZE='XSMALL' AND AUTO_SUSPEND = 300;
grant ownership on warehouse <WH_XX> to role AGERO_HOL_ROLE;


grant usage on database AGERO_HOL_DB to role AGERO_HOL_ROLE;
grant ownership on schema AGERO_HOL_DB.CALL_TRANSCRIPTS to role AGERO_HOL_ROLE;
grant ownership on schema AGERO_HOL_DB.IMAGE_CLASSIFICATION to role AGERO_HOL_ROLE;
grant ownership on schema AGERO_HOL_DB.ML_FUNCTIONS to role AGERO_HOL_ROLE;


------Forecasting------

GRANT CREATE SNOWFLAKE.ML.FORECAST ON SCHEMA AGERO_HOL_DB.ML_FUNCTIONS TO ROLE AGERO_HOL_ROLE;

------Image Classification------

GRANT CREATE NOTEBOOK ON SCHEMA AGERO_HOL_DB.IMAGE_CLASSIFICATION TO ROLE AGERO_HOL_ROLE;

