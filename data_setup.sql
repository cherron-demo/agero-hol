use role accountadmin;
create or replace database hol;

CREATE OR REPLACE SCHEMA h3_timeseries_visualization_db.h3_timeseries_visualization_s;

CREATE OR REPLACE STAGE h3_timeseries_visualization_db.h3_timeseries_visualization_s.geostage
  URL = 's3://sfquickstarts/hol_geo_spatial_ml_using_snowflake_cortex/';

CREATE OR REPLACE FILE FORMAT geocsv TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE TABLE h3_timeseries_visualization_db.h3_timeseries_visualization_s.NY_TAXI_RIDES_COMPARE (
	H3 VARCHAR(16777216),
	PICKUP_TIME TIMESTAMP_NTZ(9),
	PICKUPS NUMBER(18,0),
	FORECAST FLOAT
) AS 
SELECT
$1, $2, $3, $4
FROM @h3_timeseries_visualization_db.h3_timeseries_visualization_s.geostage/ny_taxi_rides_compare.csv;

CREATE OR REPLACE TABLE h3_timeseries_visualization_db.h3_timeseries_visualization_s.NY_TAXI_RIDES_METRICS (
	H3 VARCHAR(16777216),
	SMAPE VARIANT
) AS
SELECT $1, $2
FROM @h3_timeseries_visualization_db.h3_timeseries_visualization_s.geostage/ny_taxi_rides_metrics.csv;

select * from h3_timeseries_visualization_db.h3_timeseries_visualization_s.NY_TAXI_RIDES_COMPARE;
select * from h3_timeseries_visualization_db.h3_timeseries_visualization_s.NY_TAXI_RIDES_METRICS;
select * from diabetes_risk_dataset;

