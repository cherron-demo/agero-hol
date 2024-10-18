USE ROLE accountadmin;
use schema utility.public;

set num_users = 3;

call utility.public.loopquery('drop database if exists HOL;', $num_users);
call utility.public.loopquery('drop user if exists userXXX;', $num_users);
call utility.public.loopquery('drop role if exists roleXXX;', $num_users);
call utility.public.loopquery('drop warehouse if exists whXXX;', $num_users);
drop database utility;

show users;