drop table if exists ANISIMOVP95YANDEXRU__DWH.global_metrics cascade;

create table if not exists ANISIMOVP95YANDEXRU__DWH.global_metrics (
date_update DATETIME not null,
currency_from INT not null,
amount_total NUMERIC(10,2) not null,
cnt_transactions INT not null,
avg_transactions_per_account NUMERIC(10,2) not null,
cnt_accounts_make_transactions INT not null
)
order by date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
partition by date_update::date
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 1, 2);