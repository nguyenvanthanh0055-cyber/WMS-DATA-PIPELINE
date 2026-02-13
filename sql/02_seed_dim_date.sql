insert into mart.dim_date(
    date_key,
    date, 
    day_of_week,
    day,
    month,
    quarter,
    year,
    is_weekend
)
select 
    to_char(d::date, 'YYYYMMDD')::int as date_key,
    d::date as date,
    extract(isodow from d)::int as day_of_week,
    extract(day from d)::int as day,
    extract(month from d)::int as month,
    extract(quarter from d)::int as quarter,
    extract(year from d):: int as year,
    (extract(isodow from d)::int in(6,7)) as is_weekend
from generate_series(date '2020-01-01', date '2035-01-01', interval '1 day') as d
on conflict (date_key) do nothing;

