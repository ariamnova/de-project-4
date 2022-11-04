-- insert into cdm.dm_courier_ledger - final view
truncate table cdm.dm_courier_ledger;

INSERT INTO cdm.dm_courier_ledger
(courier_id, courier_name, settlement_year, settlement_month, 
orders_count, orders_total_sum, rate_avg, order_processing_fee, 
courier_order_sum, courier_tips_sum, courier_reward_sum)

with coeffs as (
	select 
		courier_id,
		case 
			when rate_avg < 4 then 0.05
			when rate_avg < 4.5 and  rate_avg >= 4 then 0.07
			when rate_avg < 4.9 and  rate_avg >= 4.5 then 0.08
			else 0.1
		end as rate_coef
	from (
		select
			d.courier_id,
			avg(cast(rate as numeric(14,2))) as rate_avg
		from dds.deliveries d
		group by 1) ll
),

maint as (
	select 
		d.courier_id,
		extract(year from date(order_ts)) as settlement_year,
		extract(month from date(order_ts)) as settlement_month,
		order_id,
		case
			when rate_coef = 0.05 then GREATEST(sum_delivery*rate_coef, 100)
			when rate_coef = 0.07 then GREATEST(sum_delivery*rate_coef, 150)
			when rate_coef = 0.08 then GREATEST(sum_delivery*rate_coef, 175)
			else GREATEST(sum_delivery*rate_coef, 200)
		end as courier_sum_delivery,
		sum_delivery,
		rate,
		tip_sum,
		rate_coef
	from dds.deliveries d
	left join coeffs c 
		on d.courier_id = c.courier_id
)

select
	d.courier_id,
	c.couriers_name,
	settlement_year,
	settlement_month,
	count(order_id) as orders_count,
	sum(sum_delivery) as orders_total_sum,
	avg(cast(rate as numeric(14,2))) as rate_avg,
	sum(sum_delivery) * 0.25 as order_processing_fee,
	sum(courier_sum_delivery) as courier_order_sum,
	sum(tip_sum) as courier_tips_sum,
	sum(courier_sum_delivery) + sum(tip_sum) * 0.95 as courier_reward_sum
from maint d
left join dds.couriers c 
	on d.courier_id = c.courier_id 
group by 1,2,3,4