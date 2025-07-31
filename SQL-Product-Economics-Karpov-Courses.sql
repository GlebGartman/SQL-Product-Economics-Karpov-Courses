-- Задание 1: Динамика выручки

WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   )
   

 
 SELECT date, revenue, total_revenue, ROUND((revenue - pred) * 100 / pred::NUMERIC, 2) as revenue_change FROM
  (SELECT date, revenue, sum(revenue) over(order by date) as total_revenue,lag(revenue, 1) over(order by date) as pred FROM
   (SELECT creation_time::DATE as date, sum(price) as revenue FROM  
     (SELECT order_id, creation_time, UNNEST(product_ids) as product_id FROM orders
      WHERE order_id in (SELECT * FROM plat)
      order by order_id) as tovars 
     JOIN products as p on p.product_id = tovars.product_id
    group by date  
    order by date) as stoimost) as vyruchka
    
    
    
-- Задание 2: ARPU, ARPPU и AOV
    
 WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   )
   

SELECT date,
  ROUND(revenue::NUMERIC / kolvo_user::NUMERIC, 2) as arpu,
  ROUND(revenue / plat_user::NUMERIC, 2) as arppu,
  ROUND(revenue / plat_zakaz::NUMERIC, 2) as aov
  FROM  
   (SELECT date,
    sum(revenue) FILTER (WHERE order_id in (SELECT * FROM plat)) as revenue,
    count(DISTINCT user_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as plat_user,
    count(DISTINCT user_id) as kolvo_user,
    count(DISTINCT order_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as plat_zakaz
    FROM
     (SELECT time::DATE as date, user_id, ua.order_id, action, revenue FROM
      (SELECT  order_id, sum(price) as revenue FROM  
       (SELECT order_id, creation_time, UNNEST(product_ids) as product_id FROM orders) as tovars 
        LEFT JOIN products as p on p.product_id = tovars.product_id
        group by order_id) as product_orders
       LEFT JOIN user_actions as ua on ua.order_id = product_orders.order_id
       order by date) as zakazy
   group by date) as metrics

   
   
-- Задание 3: Накопленные метрики — Running ARPU, ARPPU, AOV
   
 WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   ), 
   
  unique_day_users_pay as (SELECT date, COUNT(user_id) AS new_paying_users 
    FROM ( 
        SELECT user_id, MIN(time::date) AS date 
        FROM user_actions 
        WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order') 
        GROUP BY user_id 
    ) as data GROUP BY date),
   
 unique_day_users as 
 (
SELECT time_user, count(time_user) FILTER(WHERE porydok = 1) as new_users, count(time_user) FILTER(WHERE numer = 1) as new_users_plat FROM 
 (SELECT user_id, order_id, time::date AS time_user, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time) AS porydok,
   CASE 
     WHEN order_id IN (SELECT order_id FROM plat) THEN 
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time)
   END AS numer
 FROM user_actions
    order by user_id) as porydok_users
 group by time_user
 order by time_user)

  SELECT date,
  ROUND(sum_nakop::NUMERIC / kolvo_user_nakop::NUMERIC, 2) as running_arpu,
  ROUND(sum_nakop / plat_user_nakop::NUMERIC, 2) as running_arppu,
  ROUND(sum_nakop / plat_zakaz_nakop::NUMERIC, 2) as running_aov
  FROM  
   (
   SELECT date,
    sum(revenue) over(order by date) as sum_nakop,
    sum(plat_user) over(order by date) as plat_user_nakop,
    sum(kolvo_user) over(order by date) as kolvo_user_nakop,
    sum(plat_zakaz) over(order by date) as plat_zakaz_nakop
    FROM
    (SELECT zakazy.date as date,
      sum(revenue) FILTER (WHERE order_id in (SELECT * FROM plat)) as revenue,
      new_users as kolvo_user,
      new_paying_users  as plat_user,
      count(DISTINCT order_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as plat_zakaz
      FROM
       (SELECT time::DATE as date, user_id, ua.order_id, action, revenue FROM
        (SELECT  order_id, sum(price) as revenue FROM  
         (SELECT order_id, creation_time, UNNEST(product_ids) as product_id FROM orders) as tovars 
          LEFT JOIN products as p on p.product_id = tovars.product_id
          group by order_id) as product_orders
         LEFT JOIN user_actions as ua on ua.order_id = product_orders.order_id
         order by date) as zakazy
     JOIN  unique_day_users on unique_day_users.time_user = zakazy.date
     JOIN unique_day_users_pay on unique_day_users_pay.date = zakazy.date
     group by zakazy.date, new_users,new_paying_users) as nakoplen) as metrics  
   
     
     
 -- Задание 4: Метрики ARPU / ARPPU / AOV по дням недели
 
 WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   )
   
  SELECT
  weekday,
  weekday_number, 
  ROUND(revenue::NUMERIC / kolvo_user::NUMERIC, 2) as arpu,
  ROUND(revenue / plat_user::NUMERIC, 2) as arppu,
  ROUND(revenue / plat_zakaz::NUMERIC, 2) as aov
  FROM  
   (SELECT weekday, weekday_number, 
    sum(revenue) FILTER (WHERE order_id in (SELECT * FROM plat)) as revenue,
    count(DISTINCT user_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as plat_user,
    count(DISTINCT user_id) as kolvo_user,
    count(DISTINCT order_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as plat_zakaz
      FROM
      (SELECT to_char(date, 'Day') as weekday, DATE_PART('ISODOW', date) as weekday_number, date, user_id, order_id, action, revenue FROM
       (SELECT time::DATE as date, user_id, ua.order_id, action, revenue FROM
        (SELECT  order_id, sum(price) as revenue FROM  
         (SELECT order_id, creation_time, UNNEST(product_ids) as product_id FROM orders) as tovars 
          LEFT JOIN products as p on p.product_id = tovars.product_id
          group by order_id) as product_orders
         LEFT JOIN user_actions as ua on ua.order_id = product_orders.order_id
         order by date) as zakazy
       WHERE date >= '2022-08-26' and date < '2022-09-09') as dni
      group by weekday_number, weekday) as metrics
     order by weekday_number, weekday
     
     
     
  -- Задание 5: Доля выручки от новых и старых пользователей
     
 WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   ),
   
unique_day_users_pay as (
SELECT user_id, MIN(time::date) AS date 
        FROM user_actions 
        GROUP BY user_id 
    )
  
   
  SELECT date, 
  revenue,
  new_users_revenue,
  ROUND(new_users_revenue * 100 / revenue::NUMERIC, 2) as new_users_revenue_share,
  ROUND((revenue - new_users_revenue) * 100 / revenue::NUMERIC, 2) as old_users_revenue_share
  FROM
   (SELECT date, sum(revenue) as revenue, sum(revenue) FILTER(WHERE pay_user_id IS NOT NULL) as new_users_revenue FROM 
     (SELECT time::DATE as date, ua.user_id as user_id, uap.user_id as pay_user_id, ua.order_id, revenue FROM 
      (SELECT order_id, sum(price) as revenue FROM
       (SELECT order_id, UNNEST(product_ids) as product_id FROM orders
        WHERE order_id in (SELECT * FROM plat) 
        order by order_id) as tovars
       JOIN products as p on p.product_id = tovars.product_id
       group by order_id) as zakazy
     JOIN user_actions as ua on ua.order_id = zakazy.order_id
     LEFT JOIN unique_day_users_pay as uap on uap.user_id = ua.user_id and uap.date = ua.time::DATE
    order by user_id, date) as stoimost
   group by date) as revenues
  order by date
  
  
  
  -- Задание 6: Выручка по товарам и категория "ДРУГОЕ"
  
  WITH plat as (
SELECT order_id
FROM user_actions
group by order_id 
HAVING count(order_id) = 1
order by order_id 
)


  SELECT product_name, sum(revenue) as revenue, sum(procent) as share_in_revenue FROM 
   (SELECT 
    CASE    
    WHEN procent < 0.5 THEN 'ДРУГОЕ'  
    ELSE name
    END as product_name,
    revenue,
    procent 
    FROM
   (SELECT name, revenue, ROUND(revenue * 100 / summa::NUMERIC, 2) as procent FROM
    (SELECT name, revenue, sum(revenue) over() as summa FROM 
      (SELECT name, sum(price) as revenue FROM
       (SELECT order_id, UNNEST(product_ids) as product_id FROM orders
        WHERE order_id in (SELECT * FROM plat)
        order by order_id) as tovars
      JOIN  products as p on p.product_id = tovars.product_id
      group by name) as prices) as vyruchka) as procents) as category
    group by product_name
    order by revenue desc
    
    
 
 -- Задание 7: Выручка, затраты и валовая прибыль
    
 WITH plat as (
SELECT order_id
FROM user_actions
group by order_id 
HAVING count(order_id) = 1
order by order_id 
), 

nalog as (SELECT order_id, sum(price) as price, sum(nds) as nds 
 FROM
  (SELECT order_id, name, price,
  CASE 
  WHEN name in ('сахар', 'сухарики', 'сушки', 'семечки', 
'масло льняное', 'виноград', 'масло оливковое', 
'арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 
'овсянка', 'макароны', 'баранина', 'апельсины', 
'бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая', 
'мука', 'шпроты', 'сосиски', 'свинина', 'рис', 
'масло кунжутное', 'сгущенка', 'ананас', 'говядина', 
'соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки', 
'груши', 'лепешка', 'молоко', 'курица', 'лаваш', 'вафли', 'мандарины')

THEN ROUND(price  / 11::NUMERIC, 2)
ELSE ROUND(price  / 6::NUMERIC, 2)
END as NDS
FROM
   (SELECT order_id, name, price FROM 
        (SELECT order_id, UNNEST(product_ids) as product_id FROM orders
         WHERE order_id in (SELECT * FROM plat)
         order by order_id) as tovars
       JOIN products as p on p.product_id = tovars.product_id
       order by order_id) as names) as spisok
 group by order_id),
 
--ВАЖНАЯ ТАБЛИЦА!!!
nakop as  (SELECT  date, revenue, tax, total_revenue, total_tax, zatrata_na_sborka + zatrata_v_day as zatraty_users
  FROM
  (SELECT date, price as revenue, nds as tax,
   sum(price) over(order by date) as total_revenue, sum(nds) over(order by date) total_tax, 
   kolvo,
   CASE   
   WHEN DATE_PART('month', date) = 8 THEN kolvo * 140
   ELSE kolvo * 115
   END as zatrata_na_sborka,
   CASE   
   WHEN DATE_PART('month', date) = 8 THEN 120000
   ELSE 150000
   END as zatrata_v_day
   FROM   
  (SELECT date, sum(price) as price, sum(nds) as nds, count(order_id) as kolvo FROM
     (SELECT user_id, ua.order_id as order_id,  time::DATE as date, price, nds FROM nalog as n    
      JOIN user_actions as ua on ua.order_id = n.order_id
      order by user_id) as summa
    group by date
    order by date) as zarabotok) as money
    ),
    
couriers as (SELECT courier_id, order_id, time::DATE as date FROM courier_actions
   WHERE order_id in (SELECT * FROM plat) and action = 'deliver_order'
   order by courier_id),
   

 zatrata_na_couriera as (SELECT date, kolvo * 150 as zatrata_na_kyry   
    FROM
    (SELECT date, count(order_id) as kolvo FROM couriers
     group by date
     order by date) as chislo),
     
   
 top_five as (SELECT date, 
  CASE   
  WHEN DATE_PART('month', date) = 8 THEN kolvo_top * 400
   ELSE kolvo_top * 500
   END as zatrata_na_top_5
   FROM
   (SELECT date, count(kolvo) as kolvo_top
    FROM 
    (SELECT courier_id, date, count(order_id) as kolvo FROM couriers
     group by courier_id, date
     HAVING count(order_id) >= 5  
     order by date) as top
    group by date
    order by date) as top_5),
    
  --ВАЖНАЯ ТАБЛИЦА!!!   
 zatrata_couriers as (SELECT date, zatrata_na_kyry + zatrata_na_top_5 as zatrata_day_couriers FROM
   (SELECT znc.date, zatrata_na_kyry, COALESCE(zatrata_na_top_5, 0) as zatrata_na_top_5 FROM zatrata_na_couriera AS znc
     LEFT JOIN  top_five as tf on tf.date = znc.date) as zatraty_obchie_couries)
   
  
  SELECT date,
   revenue, costs, tax,
   gross_profit,
   total_revenue, total_costs, total_tax, 
   total_gross_profit,
   gross_profit_ratio, 
   ROUND(total_gross_profit * 100 / total_revenue::NUMERIC, 2) as total_gross_profit_ratio
   FROM
   (SELECT date,
   revenue, costs, tax,
   gross_profit,
   total_revenue, total_costs, total_tax, 
   sum(gross_profit) over(order by date) as total_gross_profit,
   ROUND(gross_profit * 100 / revenue::NUMERIC, 2) as gross_profit_ratio
   FROM
    (SELECT  date, revenue, tax, costs,
     revenue - tax - costs as gross_profit,
     total_revenue, total_tax, sum(costs) over(order by date) as total_costs
     FROM 
      (SELECT n.date as date, revenue, tax, total_revenue, total_tax, (zatraty_users + zatrata_day_couriers)::NUMERIC as costs FROM nakop as n   
       JOIN zatrata_couriers as zc on zc.date = n.date) as podvodka) as podchet) as metrics   