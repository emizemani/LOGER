select  
   substr(w.w_warehouse_name,1,20)
  ,sm.sm_type
  ,ws2.ws_name
  ,sum(case when (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (ws.ws_ship_date_sk - ws.ws_sold_date_sk > 30) and 
                 (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (ws.ws_ship_date_sk - ws.ws_sold_date_sk > 60) and 
                 (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (ws.ws_ship_date_sk - ws.ws_sold_date_sk > 90) and
                 (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (ws.ws_ship_date_sk - ws.ws_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   web_sales ws
  ,warehouse w
  ,ship_mode sm
  ,web_site ws2
  ,date_dim dd
where
    dd.d_month_seq between 1190 and 1190 + 11
and ws.ws_ship_date_sk   = dd.d_date_sk
and ws.ws_warehouse_sk   = w.w_warehouse_sk
and ws.ws_ship_mode_sk   = sm.sm_ship_mode_sk
and ws.ws_web_site_sk    = ws2.ws_web_site_sk
group by
   substr(w.w_warehouse_name,1,20)
  ,sm.sm_type
  ,ws2.ws_name
order by substr(w.w_warehouse_name,1,20)
        ,sm.sm_type
       ,ws2.ws_name
limit 100;
