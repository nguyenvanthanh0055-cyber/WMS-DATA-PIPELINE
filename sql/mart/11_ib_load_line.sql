delete from mart.fact_ib_receipt_line l
using tmp_changed t
where l.receipt_id = t.id;

with src as (
    select s.id as receipt_id, s.updated_at as stg_updated_at, s.payload::jsonb as p
    from stg.ib_receipts s
    join tmp_changed t on t.id = s.id
)

insert into mart.fact_ib_receipt_line(
  receipt_id, line_id,
  client_key, warehouse_key, product_key,
  po_date_key,
  qty_unit_id, expected_qty, actual_qty,
  _stg_updated_at
)
select
  src.receipt_id,
  nullif(line->>'line_id','') as line_id,

  dc.client_key,
  dw.warehouse_key,
  dp.product_key,

  case when nullif(src.p->>'po_date','') is not null
       then to_char((src.p->>'po_date')::date, 'YYYYMMDD')::int end as po_date_key,

  nullif(line->>'qty_unit_id','') as qty_unit_id,
  nullif(line->>'expected_qty','')::numeric as expected_qty,
  nullif(line->>'actual_qty','')::numeric as actual_qty,

  src.stg_updated_at
from src
cross join lateral jsonb_array_elements((src.p->>'lines_json')::jsonb) as line
left join mart.dim_client dc on dc.client_id = src.p->>'client_id'
left join mart.dim_warehouse dw on dw.warehouse_id = src.p->>'warehouse_id'
left join mart.dim_product dp on dp.product_id = line->>'product_id'
where nullif(line->>'line_id','') is not null;
