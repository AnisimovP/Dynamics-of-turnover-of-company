INSERT INTO ANISIMOVP95YANDEXRU__DWH.global_metrics (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
SELECT
    t.transaction_dt::date AS date_update,
    t.currency_code AS currency_from,
    SUM(CASE
            WHEN t.status = 'done' THEN
                CASE
                    WHEN t.currency_code = 420 THEN t.amount
                    ELSE t.amount * c.currency_with_div
                END
            ELSE 0
        END) AS amount_total,
    COUNT(t.operation_id) AS cnt_transactions,
    SUM(t.amount) / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM
    ANISIMOVP95YANDEXRU__STAGING.transactions t
JOIN
    ANISIMOVP95YANDEXRU__STAGING.currency c ON c.currency_code = t.currency_code
WHERE
    t.status = 'done' -- Учитываем только транзакции со статусом 'done'
    AND t.account_number_from > 0
GROUP BY
    t.transaction_dt::date,
    t.currency_code
ORDER BY
    t.transaction_dt::date,
    t.currency_code;