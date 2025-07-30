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













</details>
