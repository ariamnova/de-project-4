-- Form DDS stage;
-- Insert into restaurants table
INSERT INTO dds.restaurants
(rest_name)
select
	distinct restaurant_name
from (
	select 
	workflow_value::json ->> 'name' as restaurant_name
	from stg.restaurants) ll
	
-- Insert into couriers table
INSERT INTO dds.couriers
(courier_id, couriers_name)
select
	distinct courier_id, couriers_name
from (
	select 
	workflow_value::json ->> '_id' as courier_id,
	workflow_value::json ->> 'name' as couriers_name
	from stg.couriers) ll


-- Insert into deliveries table
INSERT INTO dds.deliveries
(order_id, order_ts, courier_id, rate, sum_delivery, tip_sum)

select 
	distinct order_id, 
	TO_TIMESTAMP(order_ts,'YYYY-MM-DD HH24:MI:SS') as order_ts, 
	courier_id, 
	cast(rate as integer) as rate, 
	cast(sum_delivery as numeric) as sum_delivery, 
	cast(tip_sum as numeric) as tip_sum
from (
	select 
		workflow_value::json ->> 'order_id' as order_id,
		workflow_value::json ->> 'order_ts' as order_ts,
		workflow_value::json ->> 'courier_id' as courier_id,
		workflow_value::json ->> 'rate' as rate,
		workflow_value::json ->> 'sum' as sum_delivery,
		workflow_value::json ->> 'tip_sum' as tip_sum,
		workflow_value
	from stg.deliveries
	) ll;