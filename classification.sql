-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role roleXXX;
use warehouse whXXX;
use database hol;
use schema schemaXXX;

-- Inspect the first 10 rows of your prediction data. This is the data the model
-- will use to generate predictions.
select * from DIABETES_RISK_DATASET limit 10;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION classification_model(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'DIABETES_RISK_DATASET'),
    TARGET_COLNAME => 'DIABETES_RISK',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Generate predictions as new columns in to your prediction table.
CREATE or REPLACE TABLE My_classification_2024_10_11 AS SELECT
    *, 
    classification_model!PREDICT(
        OBJECT_CONSTRUCT(*),
        -- This option alows the prediction process to complete even if individual rows must be skipped.
        {'ON_ERROR': 'SKIP'}
    ) as predictions
from DIABETES_RISK_DATASET;

-- View your predictions.
SELECT * FROM My_classification_2024_10_11;

-- Parse the prediction results into separate columns. 
-- Note: This is a just an example. Be sure to update this to reflect 
-- the classes in your dataset.
SELECT * EXCLUDE predictions,
        predictions:class AS class,
        round(predictions['probability'][class], 3) as probability
FROM My_classification_2024_10_11;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect your model's evaluation metrics.
CALL classification_model!SHOW_EVALUATION_METRICS();
CALL classification_model!SHOW_GLOBAL_EVALUATION_METRICS();
CALL classification_model!SHOW_CONFUSION_MATRIX();

-- Inspect the relative importance of your features, including auto-generated features.  
CALL classification_model!SHOW_FEATURE_IMPORTANCE();
