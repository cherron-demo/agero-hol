USE ROLE accountadmin;

-- Stored procedure to drop/delete all 20 warehouses
CREATE OR REPLACE PROCEDURE drop_warehouses()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    i INTEGER DEFAULT 1;
    warehouse_name STRING;
    drop_sql STRING;
BEGIN
    WHILE (i <= 20) DO
        warehouse_name := 'WH_' || LPAD(i::STRING, 2, '0');
        drop_sql := 'DROP WAREHOUSE IF EXISTS ' || warehouse_name;
        EXECUTE IMMEDIATE drop_sql;
        i := i + 1;
    END WHILE;
    RETURN 'Successfully dropped 20 warehouses (WH_01 through WH_20)';
END;
$$;

CALL drop_warehouses(); 



drop database agero_hol_db;
drop role agero_hol_rol;

show users;
show warehouses;
show roles;
