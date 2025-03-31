-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).

-- queries/revenue_by_month_year.sql
WITH stage AS (
    SELECT
        oo.order_id,
        oo.order_delivered_customer_date,
        oo.order_status,
        oop.payment_value
    FROM olist_orders oo
    JOIN olist_order_payments oop 
        ON oo.order_id = oop.order_id
)
SELECT
    strftime('%m', s.order_delivered_customer_date) AS month_no,
    CASE strftime("%m", s.order_delivered_customer_date)
        WHEN '01' THEN 'Ene'
        WHEN '02' THEN 'Feb'
        WHEN '03' THEN 'Mar'
        WHEN '04' THEN 'Abr'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'Jun'
        WHEN '07' THEN 'Jul'
        WHEN '08' THEN 'Ago'
        WHEN '09' THEN 'Sep'
        WHEN '10' THEN 'Oct'
        WHEN '11' THEN 'Nov'
        WHEN '12' THEN 'Dic'
    END AS month,
    SUM(CASE WHEN (strftime('%Y', s.order_delivered_customer_date) == '2016' AND s.order_status == 'delivered' AND s.order_delivered_customer_date IS NOT NULL) THEN s.payment_value ELSE 0.00 END) AS Year2016,
    SUM(CASE WHEN (strftime('%Y', s.order_delivered_customer_date) == '2017' AND s.order_status == 'delivered' AND s.order_delivered_customer_date IS NOT NULL) THEN s.payment_value ELSE 0.00 END) AS Year2017,
    SUM(CASE WHEN (strftime('%Y', s.order_delivered_customer_date) == '2018' AND s.order_status == 'delivered'AND s.order_delivered_customer_date IS NOT NULL) THEN s.payment_value ELSE 0.00 END) AS Year2018
FROM stage s
GROUP BY month_no, month
ORDER BY month_no;