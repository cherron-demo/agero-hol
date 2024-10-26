--------------------------- Geospatial Analytics, AI and ML using Snowflake ---------------------------

--Navigate to the Marketplace screen using the menu on the left side of the window
--Search for CARTO Academy in the search bar
--Find and click the CARTO Academy - Data for tutorials tile
--Once in the listing, click the big blue Get button

--Navigate to the Marketplace screen using the menu on the left side of the window
--Search for Worldwide Address Data in the search bar
--Find and click on the corresponding dataset from Starschema
--On the Get Data screen, don't change the name of the database from WORLDWIDE_ADDRESS_DATA.

use role accountadmin;
create or replace database hol;
use schema public;

---------------Geolocation data and Sentiment data setup---------------

CREATE OR REPLACE TABLE HOL.PUBLIC.GEOCODING_ADDRESSES AS
SELECT * 
FROM CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.DATAAPPEAL_RESTAURANTS_AND_CAFES_BERLIN_CPG
WHERE REGEXP_SUBSTR(street_address, '(\\d{5})') is not null
AND city ILIKE 'berlin';

SELECT * FROM HOL.PUBLIC.GEOCODING_ADDRESSES;

--Medium warehouse recommended. This will take ~11 minutes to run
CREATE OR REPLACE TABLE HOL.PUBLIC.OPENADDRESS AS
SELECT ST_POINT(lon, lat) as location, *
FROM WORLDWIDE_ADDRESS_DATA.ADDRESS.OPENADDRESS
WHERE lon between -180 and 180
AND lat between -90 and 90;

--This will take ~13 minutes to run
CREATE OR REPLACE TABLE HOL.PUBLIC.GEOCODING_CLEANSED_ADDRESSES as
SELECT geom, geoid, street_address, name,
    snowflake.cortex.complete('mixtral-8x7b', 
    concat('Task: Your job is to return a JSON formatted response that normalizes, standardizes, and enriches the following address,
            filling in any missing information when needed: ', street_address, 
            'Requirements: Return only in valid JSON format (starting with { and ending with }).
            The JSON response should include the following fields:
            "number": <<house_number>>,
            "street": <<street_name>>,
            "city": <<city_name>>,
            "postcode": <<postcode_value>>,
            "country": <<ISO_3166-1_alpha-2_country_code>>.
            Values inside <<>> must be replaced with the corresponding details from the address provided.
            - If a value cannot be determined, use "Null".
            - No additional fields or classifications should be included beyond the five categories listed.
            - Country code must follow the ISO 3166-1 alpha-2 standard.
            - Do not add comments or any other non-JSON text.
            - Use Latin characters for street names and cities, avoiding Unicode alternatives.
            Examples:
            Input: "123 Mn Stret, San Franscico, CA"
            Output: {"number": "123", "street": "Main Street", "city": "San Francisco", "postcode": "94105", "country": "US"}
            Input: "45d Park Avnue, New Yrok, NY 10016"
            Output: {"number": "45d", "street": "Park Avenue", "city": "New York", "postcode": "10016", "country": "US"}
            Input: "10 Downig Stret, Londn, SW1A 2AA, United Knidom"
            Output: {"number": "10", "street": "Downing Street", "city": "London", "postcode": "SW1A 2AA", "country": "UK"}
            Input: "4 Avneu des Champs Elyses, Paris, France"
            Output: {"number": "4", "street": "Avenue des Champs-Élysées", "city": "Paris", "postcode": "75008", "country": "FR"}
            Input: "1600 Amiphiteatro Parkway, Montain View, CA 94043, USA"
            Output: {"number": "1600", "street": "Amphitheatre Parkway", "city": "Mountain View", "postcode": "94043", "country": "US"}
            Input: "Plaza de Espana, 28c, Madird, Spain"
            Output: {"number": "28c", "street": "Plaza de España", "city": "Madrid", "postcode": "28008", "country": "ES"}
            Input: "1d Prinzessinenstrase, Berlín, 10969, Germany"
            Output: {"number": "1d", "street": "Prinzessinnenstraße", "city": "Berlin", "postcode": "10969", "country": "DE"} ')) as parsed_address 
        FROM HOL.PUBLIC.GEOCODING_ADDRESSES;

CREATE OR REPLACE TABLE HOL.PUBLIC.GEOCODING_CLEANSED_ADDRESSES AS
SELECT geoid, geom, street_address, name,
TRY_PARSE_JSON(parsed_address) AS parsed_address,
FROM HOL.PUBLIC.GEOCODING_CLEANSED_ADDRESSES;

ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKT';

SELECT TOP 10 * FROM HOL.PUBLIC.GEOCODING_CLEANSED_ADDRESSES;

SELECT * FROM HOL.PUBLIC.GEOCODING_CLEANSED_ADDRESSES
WHERE parsed_address IS NULL;

CREATE OR REPLACE TABLE HOL.PUBLIC.GEOCODED AS
SELECT 
    t1.name,
    t1.geom AS actual_location,
    t2.location AS geocoded_location, 
    t1.street_address as actual_address,
    t2.street as geocoded_street, 
    t2.postcode as geocoded_postcode, 
    t2.number as geocoded_number, 
    t2.city as geocoded_city
FROM ADVANCED_ANALYTICS.PUBLIC.GEOCODING_CLEANSED_ADDRESSES t1
LEFT JOIN ADVANCED_ANALYTICS.PUBLIC.OPENADDRESS t2
ON t1.parsed_address:postcode::string = t2.postcode
AND t1.parsed_address:number::string = t2.number
AND LOWER(t1.parsed_address:country::string) = LOWER(t2.country)
AND LOWER(t1.parsed_address:city::string) = LOWER(t2.city)
AND JAROWINKLER_SIMILARITY(LOWER(t1.parsed_address:street::string), LOWER(t2.street)) > 95;


CREATE OR REPLACE STAGE hol_stage URL = 's3://sfquickstarts/hol_geo_spatial_ml_using_snowflake_cortex/';

CREATE OR REPLACE FILE FORMAT csv_format_nocompression TYPE = csv
FIELD_OPTIONALLY_ENCLOSED_BY = '"' FIELD_DELIMITER = ',' skip_header = 1;

CREATE OR REPLACE TABLE HOL.PUBLIC.ORDERS_REVIEWS AS
SELECT  $1::NUMBER as order_id,
        $2::VARCHAR as customer_id,
        TO_GEOGRAPHY($3) as delivery_location,
        $4::NUMBER as delivery_postcode,
        $5::FLOAT as delivery_distance_miles,
        $6::VARCHAR as restaurant_food_type,
        TO_GEOGRAPHY($7) as restaurant_location,
        $8::NUMBER as restaurant_postcode,
        $9::VARCHAR as restaurant_id,
        $10::VARCHAR as review
FROM @HOL.PUBLIC.HOL_STAGE/food_delivery_reviews.csv (file_format => 'csv_format_nocompression');


CREATE OR REPLACE TABLE HOL.PUBLIC.ORDERS_REVIEWS_SENTIMENT (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID VARCHAR(16777216),
	DELIVERY_LOCATION GEOGRAPHY,
	DELIVERY_POSTCODE NUMBER(38,0),
	DELIVERY_DISTANCE_MILES FLOAT,
	RESTAURANT_FOOD_TYPE VARCHAR(16777216),
	RESTAURANT_LOCATION GEOGRAPHY,
	RESTAURANT_POSTCODE NUMBER(38,0),
	RESTAURANT_ID VARCHAR(16777216),
	REVIEW VARCHAR(16777216),
	SENTIMENT_ASSESSMENT VARCHAR(16777216),
	SENTIMENT_CATEGORIES VARCHAR(16777216)
);


COPY INTO HOL.PUBLIC.ORDERS_REVIEWS_SENTIMENT
FROM @HOL.PUBLIC.HOL_STAGE/food_delivery_reviews.csv
FILE_FORMAT = (FORMAT_NAME = csv_format_nocompression);


select * from HOL.PUBLIC.ORDERS_REVIEWS_SENTIMENT;

