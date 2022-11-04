-- Create DDS stage
-- Create restaurants table
drop table if exists dds.restaurants;
create table dds.restaurants (
	id serial constraint rest_pkey primary key,
	rest_name text
);

-- Create couriers table
drop table if exists dds.couriers;
create table dds.couriers (
	courier_id text constraint couriers_pkey primary key,
	couriers_name text
);

-- Create deliveries table
drop table if exists dds.deliveries;
create table dds.deliveries (
	id serial constraint deliveries_pkey primary key,
	order_id text,
	order_ts timestamp,
	delivery_id text,
	delivery_ts timestamp,
	courier_id text,
	rate integer,
	sum_delivery numeric(14,2),
	tip_sum numeric(14,2)
);

-- Add constraints	
alter table dds.deliveries drop constraint if exists courier_id_fkey;
alter table dds.deliveries add constraint courier_id_fkey
foreign key (courier_id) references dds.couriers on delete cascade;

alter table dds.deliveries drop constraint if exists sum_delivery_check;
alter table dds.deliveries add constraint sum_delivery_check
check (sum_delivery>0);

alter table dds.deliveries drop constraint if exists tip_sum_check;
alter table dds.deliveries add constraint tip_sum_check
check (tip_sum>=0);

alter table dds.deliveries drop constraint if exists rate_check;
alter table dds.deliveries add constraint rate_check
check (rate between 0 and 5);