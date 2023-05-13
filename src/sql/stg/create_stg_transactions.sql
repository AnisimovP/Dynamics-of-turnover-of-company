drop table if exists ANISIMOVP95YANDEXRU__STAGING.transactions cascade;

create table if not exists ANISIMOVP95YANDEXRU__STAGING.transactions (
operation_id varchar(60) not null,
account_number_from int not null,
account_number_to int not null,
currency_code int not null,
country varchar(30) not null,
status varchar(30) not null,
transaction_type varchar(30) not null,
amount int not null,
transaction_dt timestamp not null
)
order by operation_id
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES
partition by transaction_dt::date
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_dt::DATE, 1, 2);

CREATE PROJECTION ANISIMOVP95YANDEXRU__STAGING.transactions 
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
AS
 SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 FROM ANISIMOVP95YANDEXRU__STAGING.transactions
 ORDER BY transactions.operation_id
SEGMENTED BY hash(transactions.operation_id, transactions.transaction_dt) ALL NODES KSAFE 1;