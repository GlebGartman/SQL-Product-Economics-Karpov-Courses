<h3 align="center">Описание проекта</h3>
<p align="center">
Проект выполнен в рамках курса <strong>Karpov.Courses</strong> и посвящён анализу ключевых бизнес-метрик розничной сети с использованием SQL и визуализаций в Redash. Основная задача — построение аналитических отчётов по данным из базы <strong>PostgreSQL</strong> с акцентом на оценку эффективности бизнеса и экономики продукта.
В рамках проекта рассчитывались и визуализировались следующие показатели: выручка, затраты, прибыль, ARPU, ARPPU, AOV и другие ключевые метрики. Для анализа применялись SQL-запросы различной сложности, включая LEFT/RIGHT/INNER JOIN, оконные функции, агрегаты и CTE.
</p>


<details>
<summary><strong>Результаты</strong></summary>

<summary><strong>Задание 1: Динамика выручки</strong></summary>

📌 Для каждого дня в таблице `orders` рассчитаны следующие показатели:

- `revenue` — выручка, полученная в этот день  
- `total_revenue` — суммарная выручка с накоплением на текущий день  
- `revenue_change` — прирост выручки относительно предыдущего дня (в процентах)  
- `date` — дата

📊 Прирост (`revenue_change`) рассчитан в процентах и округлён до двух знаков после запятой.  
📅 Результирующая таблица отсортирована по дате по возрастанию.

### Код

```sql
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

```
### Динамика ежедневной выручки

![График: ежедневная выручка](https://drive.google.com/uc?export=view&id=1wnMQZ8oBvM9LLPocvOJPkUo2j-er-hI8)

### Динамика общей выручки (накопительно)

![График: общая выручка](https://drive.google.com/uc?export=view&id=1FgKToK7wIoRHEanw1q0oFB_yeVns8eSH)

---

<summary><strong>Задание 2: ARPU, ARPPU и AOV</strong></summary>

📌 Для каждого дня рассчитаны ключевые метрики выручки:

- `arpu` — средняя выручка на одного пользователя (Average Revenue Per User)  
- `arppu` — средняя выручка на одного платящего пользователя (Average Revenue Per Paying User)  
- `aov` — средний чек, или выручка с заказа (Average Order Value)  
- `date` — дата

💡 Все значения округлены до двух знаков после запятой.  
📅 Таблица отсортирована по дате по возрастанию.


### Код

```sql
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

```

### Динамика ARPU, ARPPU и AOV

![График: ARPU, ARPPU и AOV](https://drive.google.com/uc?export=view&id=1GBh7PlWGE_gLX7AmiZPuRZ7y0s5GwYI7)

---

<summary><strong>Задание 3: Накопленные метрики — Running ARPU, ARPPU, AOV</strong></summary>

📌 Для каждого дня рассчитаны накопительные показатели выручки:

- `running_arpu` — накопленная выручка на одного пользователя  
- `running_arppu` — накопленная выручка на одного платящего пользователя  
- `running_aov` — накопленная выручка на один заказ (средний чек)  
- `date` — дата

💡 Все значения округлены до двух знаков после запятой.  
📅 Результирующая таблица отсортирована по возрастанию даты.


### Код

```sql
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
   
--SELECT * FROM plat 
--SELECT * FROM unique_day_users
 
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

```

### Динамика накопленных показателей ARPU, ARPPU и AOV

![График: Running ARPU, ARPPU, AOV](https://drive.google.com/uc?export=view&id=1ztIPPDoIO_9OkPGshcYPdZXzQnMV-tDz)

---

<summary><strong>Задание 4: Метрики ARPU / ARPPU / AOV по дням недели</strong></summary>

📌 Для каждого дня недели в диапазоне с **26 августа по 8 сентября 2022 года** рассчитаны следующие показатели:

- `arpu` — выручка на одного пользователя  
- `arppu` — выручка на одного платящего пользователя  
- `aov` — выручка на один заказ (средний чек)  
- `weekday` — наименование дня недели (например, Monday)  
- `weekday_number` — порядковый номер дня недели (1 — Monday, 7 — Sunday)

📆 В расчёт включено **ровно по два дня каждого дня недели**.  
📊 Все значения округлены до двух знаков после запятой.  
📅 Результат отсортирован по `weekday_number`.


### Код

```sql
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

```


### Динамика ARPU, ARPPU и AOV по дням недели

![График: ARPU, ARPPU и AOV по дням недели](https://drive.google.com/uc?export=view&id=1LOUmfJ0Ok6BYfrWdnK3uV963YYFhJIdH)

---

<summary><strong>Задание 5: Доля выручки от новых и старых пользователей</strong></summary>

📌 Для каждого дня рассчитаны следующие показатели:

- `revenue` — общая выручка, полученная в этот день  
- `new_users_revenue` — выручка от заказов **новых пользователей**  
- `new_users_revenue_share` — доля выручки от новых пользователей (%)  
- `old_users_revenue_share` — доля выручки от остальных пользователей (%)  
- `date` — дата

📊 Все доли выражены в процентах и округлены до двух знаков после запятой.  
📅 Результат отсортирован по возрастанию даты.


### Код

```sql

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
 
```

### Динамика долей выручки от новых и старых пользователей

![График: доля выручки от новых и старых пользователей](https://drive.google.com/uc?export=view&id=1vaeeKo5-r8VhzXq4dtVVC83t0QVuLSAm)

---

<summary><strong>Задание 6: Выручка по товарам и категория "ДРУГОЕ"</strong></summary>

📌 Для каждого товара рассчитаны показатели за весь период:

- `product_name` — название товара  
- `revenue` — суммарная выручка от товара  
- `share_in_revenue` — доля выручки от товара в общей выручке (%)

🔢 Доля (`share_in_revenue`) округлена до двух знаков после запятой и выражена в процентах.  
🔻 Все товары с долей **менее 0.5%** объединены в категорию **ДРУГОЕ**, с суммированием их округлённых долей.  
📊 Результат отсортирован по убыванию выручки.


### Код

```sql

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

```


### Распределение выручки по товарам

![График: распределение выручки по товарам](https://drive.google.com/uc?export=view&id=1ukvUlSa2VnscRBG4WaNsa15gsD4KaF_0)

---

<summary><strong>Задание 7: Выручка, затраты и валовая прибыль</strong></summary>

📌 Для каждого дня рассчитаны следующие показатели:

- `revenue` — выручка, полученная в этот день  
- `costs` — затраты курьеров за день  
- `tax` — сумма НДС с продажи товаров за день  
- `gross_profit` — валовая прибыль за день (выручка − затраты − НДС)  
- `total_revenue` — накопленная выручка  
- `total_costs` — накопленные затраты  
- `total_tax` — накопленный НДС  
- `total_gross_profit` — накопленная валовая прибыль  
- `gross_profit_ratio` — доля валовой прибыли в выручке за день (%)  
- `total_gross_profit_ratio` — доля накопленной валовой прибыли в накопленной выручке (%)

📊 Все значения долей выражены в процентах и округлены до двух знаков после запятой.  
📅 Результат отсортирован по возрастанию даты.



### Код

```sql
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

```

### Динамика валовой прибыли и её доли в выручке  
![gross profit](https://drive.google.com/uc?export=view&id=1ukvUlSa2VnscRBG4WaNsa15gsD4KaF_0)

---

### Суммарная валовая прибыль и её доля в суммарной выручке  
![total gross profit](https://drive.google.com/uc?export=view&id=1CVtQGU1bV5H8xnGoC5VXOPo2jeqIcWiX)



</details>

<details> 

<summary><strong>Выводы</strong></summary>

📌 На основе рассчитанных показателей и визуализированных графиков был построен итоговый дашборд.

🔗 [Открыть дашборд в Redash](https://redash.public.karpov.courses/public/dashboards/SpodlHrsXdn5vTDNirtCWJcINDuch1fuqHRx0mFD?org_slug=default)

</details>
