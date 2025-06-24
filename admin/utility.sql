------Overall-------

--Make sure everyone has a Snowflake login
--give each attendee an ID (e.g. 01, 02, 03, etc)

create or replace role AGERO_HOL_ROLE; 
grant role AGERO_HOL_ROLE to role accountadmin;


-- Stored procedure to create 20 standard warehouses
CREATE OR REPLACE PROCEDURE AGERO_HOL_DB.public.create_warehouses()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    i INTEGER DEFAULT 1;
    warehouse_name STRING;
    create_sql STRING;
BEGIN
    WHILE (i <= 20) DO
        warehouse_name := 'WH_' || LPAD(i::STRING, 2, '0');
        create_sql := 'CREATE WAREHOUSE IF NOT EXISTS ' || warehouse_name || 
                     ' WITH WAREHOUSE_SIZE = ''X-SMALL'' ' ||
                     'AUTO_SUSPEND = 300 ' ||
                     'AUTO_RESUME = TRUE ' ||
                     'INITIALLY_SUSPENDED = TRUE';
        EXECUTE IMMEDIATE create_sql;
        i := i + 1;
    END WHILE;
    RETURN 'Successfully created 20 warehouses (WH_01 through WH_20)';
END;
$$;

Call AGERO_HOL_DB.public.create_warehouses();


-- Sample table with users and their assigned warehouses
CREATE OR REPLACE TABLE AGERO_HOL_DB.public.user_warehouse_mapping (
    user_email VARCHAR(100),
    warehouse_name VARCHAR(20)
);

-- Insert user data. This needs to be the Snowflake username for each attendee
INSERT INTO AGERO_HOL_DB.public.user_warehouse_mapping VALUES
    ('john.doe@company.com', 'WH_01'),
    ('jane.smith@company.com', 'WH_02'),
    ('mike.johnson@company.com', 'WH_03'),
    ('sarah.williams@company.com', 'WH_04'),
    ('david.brown@company.com', 'WH_05')('lisa.davis@company.com', 'WH_06'),
    ('robert.miller@company.com', 'WH_07'),
    ('jennifer.wilson@company.com', 'WH_08'),
    ('william.moore@company.com', 'WH_09'),
    ('elizabeth.taylor@company.com', 'WH_10'),
    ('james.anderson@company.com', 'WH_11'),
    ('mary.thomas@company.com', 'WH_12'),
    ('christopher.jackson@company.com', 'WH_13'),
    ('patricia.white@company.com', 'WH_14'),
    ('daniel.harris@company.com', 'WH_15'),
    ('linda.martin@company.com', 'WH_16'),
    ('matthew.garcia@company.com', 'WH_17'),
    ('barbara.rodriguez@company.com', 'WH_18'),
    ('anthony.lewis@company.com', 'WH_19'),
    ('susan.lee@company.com', 'WH_20');


CREATE OR REPLACE PROCEDURE AGERO_HOL_DB.public.grant_warehouse_ownership()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    grant_cursor CURSOR FOR 
        SELECT user_email, warehouse_name 
        FROM user_warehouse_mapping 
        ORDER BY warehouse_name;
    user_email STRING;
    warehouse_name STRING;
    grant_sql STRING;
BEGIN
    FOR record IN grant_cursor DO
        user_email := record.user_email;
        warehouse_name := record.warehouse_name;
        grant_sql := 'GRANT OWNERSHIP ON WAREHOUSE ' || warehouse_name || ' TO USER "' || user_email || '"';
        EXECUTE IMMEDIATE grant_sql;
    END FOR;
    RETURN 'Ownership grants completed successfully';
END;
$$;

-- Execute the stored procedure
CALL AGERO_HOL_DB.public.grant_warehouse_ownership();

-- Stored procedure to grant role to all users
CREATE OR REPLACE PROCEDURE AGERO_HOL_DB.public.grant_role_to_users()
RETURNS STRING
LANGUAGE SQL
AS
$
DECLARE
    user_cursor CURSOR FOR 
        SELECT DISTINCT user_email 
        FROM user_warehouse_mapping 
        ORDER BY user_email;
    user_email STRING;
    grant_sql STRING;
BEGIN
    FOR record IN user_cursor DO
        user_email := record.user_email;
        grant_sql := 'GRANT ROLE agero_hol_role TO USER "' || user_email || '"';
        EXECUTE IMMEDIATE grant_sql;
    END FOR;
    RETURN 'Role grants completed successfully';
END;
$;


--grant role to all users
CALL AGERO_HOL_DB.public.grant_role_to_users();


--grant usage on HOL objects
grant usage on database AGERO_HOL_DB to role AGERO_HOL_ROLE;
grant ownership on schema AGERO_HOL_DB.CALL_TRANSCRIPTS to role AGERO_HOL_ROLE;
grant ownership on schema AGERO_HOL_DB.IMAGE_CLASSIFICATION to role AGERO_HOL_ROLE;
grant ownership on schema AGERO_HOL_DB.ML_FUNCTIONS to role AGERO_HOL_ROLE;


------Forecasting------

GRANT CREATE SNOWFLAKE.ML.FORECAST ON SCHEMA AGERO_HOL_DB.ML_FUNCTIONS TO ROLE AGERO_HOL_ROLE;

------Image Classification------

GRANT CREATE NOTEBOOK ON SCHEMA AGERO_HOL_DB.IMAGE_CLASSIFICATION TO ROLE AGERO_HOL_ROLE;

