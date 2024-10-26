USE ROLE accountadmin;
use schema utility.public;

set num_users = xxx;

call utility.public.loopquery('drop database if exists HOL;', $num_users);
call utility.public.loopquery('drop user if exists userXXX;', $num_users);
call utility.public.loopquery('drop role if exists roleXXX;', $num_users);
call utility.public.loopquery('drop warehouse if exists whXXX;', $num_users);

drop database WORLDWIDE_ADDRESS_DATA;
drop database CARTO_ACADEMY__DATA_FOR_TUTORIALS;
drop database utility;

show users;
show warehouses;
show roles;
