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


</details>
