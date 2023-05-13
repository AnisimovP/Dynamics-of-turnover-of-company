drop table if exists ANISIMOVP95YANDEXRU__STAGING.currency cascade;

create table if not exists ANISIMOVP95YANDEXRU__STAGING.currency (
currency_code int not null,
date_update timestamp not null,
currency_code_with int not null,
currency_with_div numeric(10,2) not null
)
order by currency_code, currency_code_with
SEGMENTED BY HASH(currency_code, currency_code_with,date_update) ALL NODES
partition by date_update::date
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 1, 2);


CREATE PROJECTION ANISIMOVP95YANDEXRU__STAGING.currency
(
 currency_code,
 date_update,
 currency_code_with,
 currency_with_div
)
AS
 SELECT currency.currency_code,
        currency.date_update,
        currency.currency_code_with,
        currency.currency_with_div
 FROM ANISIMOVP95YANDEXRU__STAGING.currency
 ORDER BY currency.currency_code,
          currency.currency_code_with
SEGMENTED BY hash(currency.currency_code, currency.currency_code_with, currency.date_update) ALL NODES KSAFE 1;