create or replace view mart.v_kpi_ib_daily as
select
    f.created_date_key as date_key,
    d.date,
    f.client_key,
    f.warehouse_key,

    count(*) as receipts_created,
    count(*) filter (where upper(coalesce(f.status,'')) = 'FINISHED') as receipts_finished,
    count(*) filter (where upper(coalesce(f.status,'')) = 'CANCELLED') as receipts_cancelled,

    sum(f.expected_qty_total) as expected_qty_total,
    sum(f.actual_qty_total) as actual_qty_total,

    case
        when sum(f.expected_qty_total) is null or sum(f.expected_qty_total) = 0 then null
        else sum(f.actual_qty_total) / sum(f.expected_qty_total)
    end as fill_rate,

    avg(f.cycle_time_minutes) as avg_cycle_time_min
from mart.fact_ib_receipt f
join mart.dim_date d on d.date_key = f.created_date_key
group by 1,2,3,4;


create or replace view mart.v_kpi_ob_daily as
with base as (
    select
        f.created_date_key as date_key,
        d.date,
        f.client_key,
        f.warehouse_key,

        upper(coalesce(f.status,'')) as status_u,

        f.total_amount,
        f.actual_amount,

        case
        when f.actual_delivery_date is not null and f.expected_delivery_date is not null
        then (f.actual_delivery_date <= f.expected_delivery_date)
        end as is_otd,

        f.delivery_delay_days
    from mart.fact_ob_order f
    join mart.dim_date d on d.date_key = f.created_date_key
)
select
    date_key,
    date,
    client_key,
    warehouse_key,

    count(*) as orders_created,
    count(*) filter (where status_u = 'PACKED') as orders_packed,
    count(*) filter (where status_u = 'CANCELLED') as orders_cancelled,

    sum(total_amount) as total_amount_sum,
    sum(actual_amount) as actual_amount_sum,

    case
        when count(*) filter (where is_otd is not null) = 0 then null
        else (count(*) filter (where is_otd = true)::numeric / count(*) filter (where is_otd is not null))
    end as otd_rate,

    avg(delivery_delay_days) as avg_delivery_delay_days
from base
group by 1,2,3,4;


create or replace view mart.v_kpi_ib_wip_status as
select
    now()::timestamptz as snapshot_at,
    f.client_key,
    f.warehouse_key,
    coalesce(f.status, 'UNKNOWN') as status,

    count(*) as open_count,
    avg(extract(epoch from (now()::timestamptz - f.created_at)) / 3600.0) as avg_age_hours
from mart.fact_ib_receipt f    
where f.created_at is not null
and upper(coalesce(f.status,'')) not in ('FINISHED','CANCELLED')
group by 2,3,4;


create or replace view mart.v_kpi_ob_wip_status as
select
    now()::timestamptz as snapshot_at,
    f.client_key,
    f.warehouse_key,
    coalesce(f.status, 'UNKNOWN') as status,

    count(*) as open_count,
    avg(extract(epoch from (now()::timestamptz - f.created_at)) / 3600.0) as avg_age_hours
from mart.fact_ob_order f
where f.created_at is not null
    and upper(coalesce(f.status,'')) not in ('PACKED','CANCELLED')
group by 2,3,4;
