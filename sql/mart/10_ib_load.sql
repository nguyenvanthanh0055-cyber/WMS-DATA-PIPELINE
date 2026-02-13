drop table if exists tmp_changed;
create temporary table tmp_changed (
    id uuid primary key
) on commit drop;

insert into tmp_changed(id)
select id
from stg.ib_receipts
where updated_at > :wm_effective;

insert into mart.dim_client(client_id)
select distinct nullif(p->>'client_id','') as client_id
from stg.ib_receipts s
join tmp_changed t on t.id = s.id 
cross join lateral (select s.payload::jsonb) as p(p)
where nullif(p->>'client_id','') is not null
on conflict (client_id) do nothing;

insert into mart.dim_warehouse(warehouse_id)    
select distinct nullif(p ->> 'warehouse_id','') as warehouse_id
from stg.ib_receipts s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
where nullif(p->>'warehouse_id','') is not null
on conflict (warehouse_id) do nothing;

insert into mart.dim_user(username)
select distinct u.username
from stg.ib_receipts s 
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
cross join lateral (
    values
        (nullif(p->>'created_by','')),
        (nullif(p->>'updated_by',''))
) as u(username)
where u.username is not null
on conflict (username) do nothing;

insert into mart.dim_product(product_id, sku)
select distinct
    nullif(line ->> 'product_id','') as product_id,
    nullif(line ->> 'sku','') as sku
from stg.ib_receipts s
join tmp_changed t on t.id = s.id
cross join lateral (select s.payload::jsonb) as p(p)
cross join lateral jsonb_array_elements((p->>'lines_json')::jsonb) as line 
where nullif(line ->> 'product_id','') is not null
on conflict(product_id) do update
set sku = excluded.sku;


with src as (
    select 
        s.id as receipt_id,
        s.updated_at as stg_updated_at,
        s.payload::jsonb as p
    from stg.ib_receipts s
    join tmp_changed t on t.id = s.id

),
agg as (
    select 
        receipt_id,
        count(*) as line_count,
        sum(nullif(line->>'expected_qty','')::numeric) as expected_qty_total,
        sum(nullif(line->>'actual_qty','')::numeric) as actual_qty_total
    from src
    cross join lateral jsonb_array_elements((src.p->>'lines_json')::jsonb) as line
    group by receipt_id
),
upserted as (
insert into mart.fact_ib_receipt(
    receipt_id, po_code, status, client_key, warehouse_key,
    created_by_user_key, updated_by_user_key, po_date_key, 
    created_date_key, updated_date_key, finished_date_key,
    po_date, created_at, updated_at, finished_at, line_count,
    expected_qty_total, actual_qty_total, cycle_time_minutes,
    _stg_updated_at, _loaded_at
)
select 
    src.receipt_id,
    nullif(src.p->>'po_code','') as po_code,
    nullif(src.p->>'status','') as status,
    dc.client_key,
    dw.warehouse_key,
    u_created.user_key,
    u_updated.user_key,

    case
        when nullif(src.p->> 'po_date','') is not null
        then to_char((src.p ->> 'po_date')::date, 'YYYYMMDD')::int
    end as po_date_key,
    case
        when nullif(src.p->> 'created_at','') is not null
        then to_char((src.p->> 'created_at')::timestamptz, 'YYYYMMDD')::int
    end as created_date_key,
    case
        when nullif(src.p->> 'updated_at','') is not null
        then to_char((src.p->> 'updated_at')::timestamptz, 'YYYYMMDD')::int
    end as updated_date_key,
    case
        when nullif(src.p->> 'finished_at','') is not null
        then to_char((src.p->> 'finished_at')::timestamptz, 'YYYYMMDD')::int
    end as finished_date_key,
    case
        when nullif(src.p->> 'po_date','') is not null
        then (src.p->>'po_date')::date
    end as po_date,  

    nullif(src.p->>'created_at','')::timestamptz as created_at,
    nullif(src.p->>'updated_at','')::timestamptz as updated_at,
    nullif(src.p->>'finished_at',''):: timestamptz as finished_at,
    coalesce(agg.line_count,0),
    coalesce(agg.expected_qty_total,0) as expected_qty_total,
    coalesce(agg.actual_qty_total,0) as actual_qty_total,

    case
        when nullif(src.p->> 'finished_at','') is not null
        then extract(epoch from (
            (src.p ->>'finished_at')::timestamptz - (src.p ->>'created_at')::timestamptz
        )) / 60.0
    end as cycle_time_minutes,

    src.stg_updated_at as _stg_updated_at,
    now() as _loaded_at


from src
left join agg on agg.receipt_id = src.receipt_id
left join mart.dim_client dc on dc.client_id = src.p->> 'client_id'
left join mart.dim_warehouse dw on dw.warehouse_id = src.p->>'warehouse_id'
left join mart.dim_user u_created on u_created.username = src.p->>'created_by'
left join mart.dim_user u_updated on u_updated.username = src.p->>'updated_by'
on conflict(receipt_id) do update
set
    po_code = excluded.po_code,
    status = excluded.status,
    client_key = excluded.client_key,
    warehouse_key = excluded.warehouse_key,
    created_by_user_key = excluded.created_by_user_key,
    updated_by_user_key = excluded.updated_by_user_key,
    po_date_key = excluded.po_date_key,
    created_date_key = excluded.created_date_key,
    updated_date_key = excluded.updated_date_key,
    finished_date_key = excluded.finished_date_key,
    po_date = excluded.po_date,
    created_at = excluded.created_at,
    updated_at = excluded.updated_at,
    finished_at = excluded.finished_at,
    line_count = excluded.line_count,
    expected_qty_total = excluded.expected_qty_total,
    actual_qty_total = excluded.actual_qty_total,
    cycle_time_minutes = excluded.cycle_time_minutes,
    _stg_updated_at = excluded._stg_updated_at,
    _loaded_at = now()  
where 
    (mart.fact_ib_receipt.status,
    mart.fact_ib_receipt.updated_at,
    mart.fact_ib_receipt.line_count,
    mart.fact_ib_receipt.expected_qty_total,
    mart.fact_ib_receipt.actual_qty_total,
    mart.fact_ib_receipt._stg_updated_at)
is distinct from
    (excluded.status,
    excluded.updated_at,
    excluded.line_count,
    excluded.expected_qty_total,
    excluded.actual_qty_total,
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
    from stg.ib_receipts s
    join tmp_changed t on t.id = s.id
)
select 
    stats.rows_inserted,
    stats.rows_updated,
    wm.new_wm
from stats
cross join wm;
