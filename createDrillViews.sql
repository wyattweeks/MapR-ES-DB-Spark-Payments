use dfs.tmp;
create or replace view physicians_by_revenue as select physician_id, sum(amount) as revenue from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_id;
create or replace view physicians_by_specialty_revenue as select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty;
create or replace view aca_open_payments as select recipient_country, recipient_state, physician_specialty, recipient_zip, payer, nature_of_payment, (sum(amount)) as us_dollars from dfs.`/user/mapr/demo.mapr.com/tables/payments` GROUP BY recipient_country, recipient_state, recipient_zip, physician_specialty, payer, nature_of_payment;
create or replace view aca_open_payments_all as select * from dfs.`/user/mapr/demo.mapr.com/tables/payments`;