Новая витрина - cdm.dm_courier_ledger

id — идентификатор записи
courier_id — ID курьера, которому перечисляем
courier_name — Ф. И. О. курьера
settlement_year — год отчёта
settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь
orders_count — количество заказов за период (месяц)
orders_total_sum — общая стоимость заказов
rate_avg — средний рейтинг курьера по оценкам пользователей
order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25
courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы, в зависимости от рейтинга
courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых
courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
r < 4 — 5% от заказа, но не менее 100 р.;
4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
4.9 <= r — 10% от заказа, но не менее 200 р.

------------------------------------------------------------------------------------------------------------------------------------------

id — auto increment
courier_id — новые данные
courier_name — новые данные
settlement_year — dm_orders
settlement_month — dm_orders
orders_count — dm_orders
orders_total_sum — новые данные
rate_avg — новые данные
order_processing_fee — fct_product_sales
courier_order_sum — новые данные
courier_tips_sum — новые данные
courier_reward_sum — новые данные

------------------------------------------------------------------------------------------------------------------------------------------

Новые таблицы:
stg:
deliverysystem_couriers(
    id SERIAL PRIMARY KEY
    courier_id VARCHAR 
    name VARCHAR
)
deliverysystem_deliveries(
    id SERIAL PRIMARY KEY
    order_id — VARCHAR
    order_ts — TIMESTAMP
    delivery_id — VARCHAR
    courier_id — VARCHAR
    address — VARCHAR
    delivery_ts — TIMESTAMP
    rate — TINYINT
    sum — NUMERIC
    tip_sum — NUMERIC
)

dds:
dm_couriers(
    id SERIAL PRIMARY KEY
    courier_id VARCHAR 
    name VARCHAR
)

cdm:
cdm.dm_courier_ledger(
    id SERIAL PRIMARY KEY
    courier_id INT 
    courier_name VARCHAR
    settlement_year INT 
    settlement_month INT 
    orders_count INT (количество заказов за период (месяц))
    orders_total_sum NUMERIC (общая стоимость заказов)
    rate_avg NUMERIC (средний рейтинг курьера по оценкам пользователей)
    order_processing_fee NUMERIC 
    courier_order_sum NUMERIC
    courier_tips_sum NUMERIC 
    courier_reward_sum NUMERIC 
)

Обновление таблиц:
dm_orders(
    courier_id — INT
    rate — TINYINT
    sum — NUMERIC
    tip_sum — NUMERIC
)

------------------------------------------------------------------------------------------------------------------------------------------

Команды для stg:
curl -H "X-Nickname: Djammer" -H "X-Cohort: 1" -H "X-API-KEY: 25c27781-8fde-4b30-a22e-524044a7580f" --location --request GET "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={"_id"}&sort_direction={"asc"}&limit={"50"}&offset={"0"}"

curl -H "X-Nickname: Djammer" -H "X-Cohort: 1" -H "X-API-KEY: 25c27781-8fde-4b30-a22e-524044a7580f" --location --request GET "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?from=2022-01-01%2000:00:00&to=2022-01-02%2000:00:00&sort_field={"date"}&sort_direction={"asc"}&limit={"50"}&offset={"0"}"

offset смещается после каждого запроса 50

