delete from mart.fact_ob_order_line l
using tmp_changed t
where l.order_id = t.id;

insert into mart.fact_ob_order_line(
  order_id, line_id,
  client_key, warehouse_key, product_key,
  qty,
  _stg_updated_at
)
select
  src.order_id,
  nullif(line->>'line_id','') as line_id,

  dc.client_key,
  dw.warehouse_key,
  dp.product_key,

  nullif(line->>'qty','')::numeric as qty,
  src.stg_updated_at
from (
  select s.id as order_id, s.updated_at as stg_updated_at, s.payload::jsonb as p
  from stg.ob_orders s
  join tmp_changed t on t.id = s.id
) src
cross join lateral jsonb_array_elements((src.p->>'lines_json')::jsonb) as line
left join mart.dim_client dc on dc.client_id = src.p->>'client_id'
left join mart.dim_warehouse dw on dw.warehouse_id = src.p->>'warehouse_id'
left join mart.dim_product dp on dp.product_id = line->>'product_id'
where nullif(line->>'line_id','') is not null;
