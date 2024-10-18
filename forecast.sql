
use role roleXXX;
use warehouse whXXX;
use database hol;
use schema schemaXXX;
select * from cvs_sales limit 10;

CREATE OR REPLACE VIEW single_city_view AS (
    SELECT
        sales_date,
        revenue
    FROM
        cvs_sales
    WHERE
        city LIKE 'Queens'
);

select * from single_city_view;

-- Create forecast using Cortex ML-Powered Function "Forecast"
-- Documentation https://docs.snowflake.com/en/user-guide/ml-powered-
-- Example of Single-Series Data Forecast
CREATE OR REPLACE snowflake.ml.forecast single_city_model (
    INPUT_DATA => SYSTEM$REFERENCE('view', 'single_city_view'),
    TIMESTAMP_COLNAME => 'sales_date',
    TARGET_COLNAME => 'revenue'
);

-- Show models to confirm training has completed: 
SHOW snowflake.ml.forecast;

-- Forecasting model is now available as single_city_model
-- Use CALL to forecast the next thrity timestamps (30 Days):
CALL single_city_model!FORECAST(FORECASTING_PERIODS => 30);

-- Use Chart to visualize the forecast
-- Select Chart Type = Line
-- Select Data FORECAST
-- Add column UPPER_BOUND
-- Add colum LOWER_BOUND
-- Keep TS as X-Axis

-- Let's save the forecasted value predictions as a new TABLE
CREATE OR REPLACE TABLE queens_forecast_table AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

SELECT * FROM queens_forecast_table;

-- Add the forecasted value predictions (FORECAST, LOWER_BOUND, UPPER_BOUND) to the original input data
CREATE OR REPLACE VIEW combined_table AS (
    SELECT TO_DATE(sales_date) AS date, revenue, NULL as FORECAST, NULL as LOWER_BOUND, NULL AS UPPER_BOUND FROM single_city_view
    UNION 
    SELECT TO_DATE(TS) AS date, NULL as ticket_sales, FORECAST, LOWER_BOUND, UPPER_BOUND from queens_forecast_table
    ORDER BY date DESC
);

SELECT * FROM combined_table;

-- Use Chart to visualize the forecast
-- Select Chart Type = Line
-- Select Data TOTAL_SOLD
-- Add column UPPER_BOUND
-- Add column FORECAST
-- Add colum LOWER_BOUND
-- Keep DATE as X-Axis

-- Now let's think about Forecasting on Multiple Series
-- Create view for all cvs locations
-- Ensure you remove any unnecessary columns, else the model will attempt to use as exogenous variables
CREATE OR REPLACE VIEW all_cvs_locations AS (
    SELECT 
        sales_date,
        revenue,
        city      
    FROM
        cvs_sales
);

select * from all_cvs_locations;

SELECT DISTINCT city FROM all_cvs_locations;

CREATE OR REPLACE snowflake.ml.forecast all_locations_forecast (
  INPUT_DATA => SYSTEM$REFERENCE('view', 'all_cvs_locations'),
  SERIES_COLNAME => 'city',
  TIMESTAMP_COLNAME => 'sales_date',
  TARGET_COLNAME => 'revenue'
);

SHOW snowflake.ml.forecast;

-- Forecasting model is now available as all_locations_forecast
-- Use CALL to forecast the next thrity timestamps (30 Days):
CALL all_locations_forecast!FORECAST(FORECASTING_PERIODS => 30);

-- Let's save the forecasted value predictions as a new TABLE
CREATE OR REPLACE TABLE all_locations_predictions AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

SELECT * FROM all_locations_predictions;

-- Remove unnecessary quotes from the new SERIES column 
SELECT *, REGEXP_REPLACE(SERIES,'["]') as city FROM all_locations_predictions;
-- Save output as the new all_location_prediction_table table
CREATE OR REPLACE TABLE all_location_prediction_table AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

ALTER TABLE all_location_prediction_table DROP COLUMN SERIES;

select * from all_location_prediction_table;

-- Add the forecasted value predictions (FORECAST, LOWER_BOUND, UPPER_BOUND) to the original input data for multiple items
CREATE OR REPLACE VIEW all_locations_combined AS (
    SELECT TO_DATE(sales_date) AS date, city, revenue, NULL as FORECAST, NULL as LOWER_BOUND, NULL AS UPPER_BOUND FROM all_cvs_locations
    UNION 
    SELECT TO_DATE(TS) AS date, city, NULL as ticket_sales, FORECAST, LOWER_BOUND, UPPER_BOUND from all_location_prediction_table
    ORDER BY date DESC
);

SELECT * FROM all_locations_combined;

-- Use Chart to visualize the forecast
-- Select Chart Type = Line
-- Select Data FORECAST
-- Add column MENU_ITEM_NAME as SERIES
-- Keep Date as X-Axis

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL all_locations_forecast!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
-- CALL my_model!EXPLAIN_FEATURE_IMPORTANCE();
