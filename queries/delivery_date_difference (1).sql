-- TODO: Esta consulta devolverá una tabla con dos columnas: Estado y 
-- Diferencia_Entrega. La primera contendrá las letras que identifican los 
-- estados, y la segunda mostrará la diferencia promedio entre la fecha estimada 
-- de entrega y la fecha en la que los productos fueron realmente entregados al 
-- cliente.
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. Puedes usar la función CAST para convertir un número a un entero.
-- 3. Puedes usar la función STRFTIME para convertir order_delivered_customer_date a una cadena, eliminando horas, minutos y segundos.
-- 4. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL

-- queries/delivery_date_difference.sql

WITH delivery_times AS (
    SELECT
        CAST(julianday(STRFTIME('%J',oo.order_estimated_delivery_date)) AS INT) - CAST(julianday(STRFTIME('%J',oo.order_delivered_customer_date)) AS INT)  AS delivery_difference,
        cc.customer_state AS state
    FROM
        olist_orders oo
        JOIN olist_customers cc ON oo.customer_id = cc.customer_id
    WHERE
        oo.order_status == 'delivered'
        AND oo.order_delivered_customer_date IS NOT NULL
)
SELECT
    state AS State,
    ROUND(AVG(delivery_difference),0) AS Delivery_Difference
FROM
    delivery_times
GROUP BY
    state
ORDER BY
    Delivery_Difference ASC;