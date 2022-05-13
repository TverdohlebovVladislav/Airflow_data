select c.customer_id, sum(p.amount) as amount, current_timestamp as execution_timestamp
from customer as c
join payments as p on c.customer_id=p.customer_id
group by c.customer_id