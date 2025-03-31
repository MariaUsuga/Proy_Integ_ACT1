-- TODO: Esta consulta devolverá una tabla con las 10 categorías con menores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con menores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.
SELECT
    pc.product_category_name_english AS Category,
    COUNT(DISTINCT oi.order_id) AS Num_order,
    SUM(oop.payment_value) AS Revenue
FROM
    olist_order_items oi
    JOIN olist_products p ON oi.product_id = p.product_id
    JOIN olist_order_payments oop ON oi.order_id = oop.order_id
    JOIN olist_orders oo ON oo.order_id = oop.order_id
    JOIN product_category_name_translation pc ON p.product_category_name = pc.product_category_name
WHERE (oo.order_status == 'delivered' AND oo.order_delivered_customer_date IS NOT NULL)
GROUP BY
    pc.product_category_name_english
ORDER BY
    Revenue ASC 
LIMIT 10;
