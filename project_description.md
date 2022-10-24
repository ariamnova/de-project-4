# Data warehouse implementation for the couriers' settlement

### Description
Calculate the amount of payment for each courier for the preliminary month. For example, in June we aggregated the amount received in May. The settlement date is the 10th day of each month.


### View description

* `id` — row identifier.
* `courier_id` — courier's ID that will receive the payment.
* `courier_name` —courier's name.
* `settlement_year` — settlement year.
* `settlement_month` — settlement month from 1 to 12.
* `orders_count` — the number of orders per period (month).
* `orders_total_sum` — the orders' cost.
* `rate_avg` — average courier rating according to user experience.
* `order_processing_fee` — the amount withheld by the company for order processing: orders_total_sum * 0.25.
* `courier_order_sum` — the amount to be transferred to the courier for the orders delivered by him/her/they. The courier must receive a certain amount for each delivered order depending on the rating.
* `courier_tips_sum` — courier tip amount.
* `courier_reward_sum` — the amount to be transferred to the courier: courier_order_sum + courier_tips_sum * 0.95 (5% — payment processing fee).


### The percentage of the payment
Depends on the courier's rating, where r is the average rating of the courier in the current month:
* r < 4 - 5% of the order cost, but not less than 100;
* 4 <= r < 4.5 - 7% of the order cost, but not less than 150;
* 4.5 <= r < 4.9 - 8% of the order cost, but not less than 175;
* 4.9 <= r - 10% of the order cost, but not less than 200.

### Структура репозитория

