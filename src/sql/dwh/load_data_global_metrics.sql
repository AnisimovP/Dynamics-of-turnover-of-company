insert into ANISIMOVP95YANDEXRU__DWH.global_metrics
select 			t.transaction_dt 		as date_update,
				t.currency_code 		as currency_from,
				sum(CASE when c.currency_code = 420
							then t.amount
							else t.amount * c.currency_with_div
				END
				) as amount_total,
				count(t.operation_id) as cnt_transactions,
				sum(t.amount) / count(distinct(t.account_number_from)) AS avg_transactions_per_account,
				COUNT(DISTINCT CASE WHEN t.account_number_from >= 0 THEN t.account_number_from END) AS cnt_accounts_make_transactions
from ANISIMOVP95YANDEXRU__STAGING.transactions t 
join ANISIMOVP95YANDEXRU__STAGING.currency c on c.currency_code =t.currency_code 
where account_number_from > 0
GROUP by 	t.transaction_dt,
			t.currency_code
ORDER BY 	t.transaction_dt,
			t.currency_code;