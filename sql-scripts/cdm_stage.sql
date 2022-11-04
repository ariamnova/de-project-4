--create final view CDM
drop table if exists cdm.dm_courier_ledger;
create table cdm.dm_courier_ledger (
	id serial constraint id_final_view_pkey primary key,
	courier_id text, 
	courier_name text, 
	settlement_year integer,
	settlement_month integer,
	orders_count integer,
	orders_total_sum numeric(14,2),
	rate_avg numeric(14,3),
	order_processing_fee numeric(14,2),
	courier_order_sum numeric(14,2),
	courier_tips_sum numeric(14,2),
	courier_reward_sum numeric(14,2)
);