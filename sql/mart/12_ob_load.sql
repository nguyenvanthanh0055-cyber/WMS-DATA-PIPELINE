drop table if exists tmp_changed;
create temporary table tmp_changed (id uuid primary key) on commit drop;

insert into tmp_changed(id)
select id
from stg.ob_orders
where updated_at > :wm_effective;

insert into mart.dim_client(client_id)
select distinct nullif(p->>'client_id','') as client_id
from stg.ob_orders s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
where nullif(p->>'client_id','') is not null
on conflict (client_id) do nothing;

insert into mart.dim_warehouse(warehouse_id)
select distinct nullif(p->>'warehouse_id','') as warehouse_id
from stg.ob_orders s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
where nullif(p->>'warehouse_id','') is not null
on conflict (warehouse_id) do nothing;

insert into mart.dim_user(username)
select distinct u.username
from stg.ob_orders s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
cross join lateral (
  values
    (nullif(p->>'created_by','')),
    (nullif(p->>'updated_by',''))
) as u(username)
where u.username is not null
on conflict (username) do nothing;

insert into mart.dim_customer(customer_id)
select distinct nullif(p->>'customer_id','') as customer_id
from stg.ob_orders s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
where nullif(p->>'customer_id','') is not null
on conflict (customer_id) do nothing;

insert into mart.dim_shipping_address(shipping_address_id)
select distinct nullif(p->>'shipping_address_id','') as shipping_address_id
from stg.ob_orders s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
where nullif(p->>'shipping_address_id','') is not null
on conflict (shipping_address_id) do nothing;

insert into mart.dim_product(product_id, sku)
select distinct
  nullif(line->>'product_id','') as product_id,
  nullif(line->>'sku','') as sku
from stg.ob_orders s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
cross join lateral jsonb_array_elements((p->>'lines_json')::jsonb) as line
where nullif(line->>'product_id','') is not null
on conflict (product_id) do update
set sku = excluded.sku;

with src as (
  select
    s.id as order_id,
    s.updated_at as stg_updated_at,
    s.payload::jsonb as p
  from stg.ob_orders s
  join tmp_changed t on t.id = s.id
),
agg as (
  select
    order_id,
    count(*) as line_count
  from src
  cross join lateral jsonb_array_elements((src.p->>'lines_json')::jsonb) as line
  group by order_id
),
upserted as (
insert into mart.fact_ob_order(
  order_id, so_code, status,
  client_key, warehouse_key,
  created_by_user_key, updated_by_user_key,
  customer_key, shipping_address_key,
  expected_delivery_date_key, actual_delivery_date_key,
  created_date_key, updated_date_key,
  expected_delivery_date, actual_delivery_date,
  created_at, updated_at,
  line_count,
  total_amount, actual_amount, total_cod_amount, total_weight, total_volume,
  delivery_delay_days,
  _stg_updated_at, _loaded_at
)
select
  src.order_id,
  nullif(src.p->>'so_code','') as so_code,
  nullif(src.p->>'status','') as status,

  dc.client_key,
  dw.warehouse_key,

  u_created.user_key,
  u_updated.user_key,

  cus.customer_key,
  addr.shipping_address_key,

  case when nullif(src.p->>'expected_delivery_date','') is not null
       then to_char((src.p->>'expected_delivery_date')::date, 'YYYYMMDD')::int end,

  case when nullif(src.p->>'actual_delivery_date','') is not null
       then to_char((src.p->>'actual_delivery_date')::date, 'YYYYMMDD')::int end,

  case when nullif(src.p->>'created_at','') is not null
       then to_char(((src.p->>'created_at')::timestamptz)::date, 'YYYYMMDD')::int end,

  case when nullif(src.p->>'updated_at','') is not null
       then to_char(((src.p->>'updated_at')::timestamptz)::date, 'YYYYMMDD')::int end,

  case when nullif(src.p->>'expected_delivery_date','') is not null
       then (src.p->>'expected_delivery_date')::date end,

  case when nullif(src.p->>'actual_delivery_date','') is not null
       then (src.p->>'actual_delivery_date')::date end,

  nullif(src.p->>'created_at','')::timestamptz as created_at,
  nullif(src.p->>'updated_at','')::timestamptz as updated_at,

  coalesce(agg.line_count,0),

  nullif(src.p->>'total_amount','')::numeric,
  nullif(src.p->>'actual_amount','')::numeric,
  nullif(src.p->>'total_cod_amount','')::numeric,
  nullif(src.p->>'total_weight','')::numeric,
  nullif(src.p->>'total_volume','')::numeric,

  case
    when nullif(src.p->>'actual_delivery_date','') is not null and nullif(src.p->>'expected_delivery_date','') is not null
    then ((src.p->>'actual_delivery_date')::date - (src.p->>'expected_delivery_date')::date)::int
  end as delivery_delay_days,

  src.stg_updated_at as _stg_updated_at,
  now() as _loaded_at
from src
left join agg on agg.order_id = src.order_id
left join mart.dim_client dc on dc.client_id = src.p->>'client_id'
left join mart.dim_warehouse dw on dw.warehouse_id = src.p->>'warehouse_id'
left join mart.dim_user u_created on u_created.username = src.p->>'created_by'
left join mart.dim_user u_updated on u_updated.username = src.p->>'updated_by'
left join mart.dim_customer cus on cus.customer_id = src.p->>'customer_id'
left join mart.dim_shipping_address addr on addr.shipping_address_id = src.p->>'shipping_address_id'
on conflict (order_id) do update
set
  so_code = excluded.so_code,
  status = excluded.status,
  client_key = excluded.client_key,
  warehouse_key = excluded.warehouse_key,
  created_by_user_key = excluded.created_by_user_key,
  updated_by_user_key = excluded.updated_by_user_key,
  customer_key = excluded.customer_key,
  shipping_address_key = excluded.shipping_address_key,
  expected_delivery_date_key = excluded.expected_delivery_date_key,
  actual_delivery_date_key = excluded.actual_delivery_date_key,
  created_date_key = excluded.created_date_key,
  updated_date_key = excluded.updated_date_key,
  expected_delivery_date = excluded.expected_delivery_date,
  actual_delivery_date = excluded.actual_delivery_date,
  created_at = excluded.created_at,
  updated_at = excluded.updated_at,
  line_count = excluded.line_count,
  total_amount = excluded.total_amount,
  actual_amount = excluded.actual_amount,
  total_cod_amount = excluded.total_cod_amount,
  total_weight = excluded.total_weight,
  total_volume = excluded.total_volume,
  delivery_delay_days = excluded.delivery_delay_days,
  _stg_updated_at = excluded._stg_updated_at,
  _loaded_at = now()
where 
    (mart.fact_ob_order.status,
    mart.fact_ob_order.updated_at,
    mart.fact_ob_order.line_count,
    mart.fact_ob_order.total_amount,
    mart.fact_ob_order.actual_amount,
    mart.fact_ob_order.total_cod_amount,
    mart.fact_ob_order.total_weight,
    mart.fact_ob_order.total_volume,
    mart.fact_ob_order._stg_updated_at)
is distinct from
    (excluded.status,
    excluded.updated_at,
    excluded.line_count,
    excluded.total_amount,
    excluded.actual_amount,
    excluded.total_cod_amount,
    excluded.total_weight,
    excluded.total_volume,
    excluded._stg_updated_at)

returning (xmax = 0) as inserted
),
stats as (
  select
    count(*) filter (where inserted)     as rows_inserted,
    count(*) filter (where not inserted) as rows_updated
  from upserted
),
wm as (
  select max(s.updated_at) as new_wm
  from stg.ob_orders s
  join tmp_changed t on t.id = s.id
)
select 
    stats.rows_inserted,
    stats.rows_updated,
    wm.new_wm
from stats
cross join wm;
