--create staging table restaurant
drop table if exists stg.restaurants;
create table stg.restaurants (
	id serial constraint rest_pkey primary key,
	workflow_value text UNIQUE
);

--create staging table couriers
drop table if exists stg.couriers;
create table stg.couriers (
	id serial constraint cour_pkey primary key,
	workflow_value text UNIQUE
);

--create staging table deliveries
drop table if exists stg.deliveries;
create table stg.deliveries (
	id serial constraint deliv_pkey primary key,
	workflow_value text unique,
	from_date timestamp,
	to_date timestamp
);