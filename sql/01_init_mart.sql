create schema if not exists mart;

create table if not exists mart.dim_date(
    date_key int primary key,
    date date not null,
    day_of_week int not null,
    day int not null,
    month int not null,
    quarter int not null,
    year int not null,
    is_weekend boolean not null
);

create table if not exists mart.dim_client(
    client_key bigserial primary key,
    client_id text not null unique
);

create table if not exists mart.dim_warehouse(
    warehouse_key bigserial primary key,
    warehouse_id text not null unique
);

create table if not exists mart.dim_product(
    product_key bigserial primary key,
    product_id text not null unique,
    sku text
);

create table if not exists mart.dim_user(
    user_key bigserial primary key,
    username text not null unique
);

create table if not exists mart.dim_customer(
    customer_key bigserial primary key,
    customer_id text not null unique
);

create table if not exists mart.dim_shipping_address(
    shipping_address_key bigserial primary key,
    shipping_address_id text not null unique
);

create table if not exists mart.fact_ib_receipt(
    receipt_id uuid primary key,
    po_code text,
    status text,

    client_key bigint references mart.dim_client(client_key),
    warehouse_key bigint references mart.dim_warehouse(warehouse_key),

    created_by_user_key bigint references mart.dim_user(user_key),
    updated_by_user_key bigint references mart.dim_user(user_key),

    po_date_key int references mart.dim_date(date_key),
    created_date_key int references mart.dim_date(date_key),
    updated_date_key int references mart.dim_date(date_key),
    finished_date_key int references mart.dim_date(date_key),

    po_date date,
    created_at timestamptz,
    updated_at timestamptz,
    finished_at timestamptz,

    line_count int not null default 0,
    expected_qty_total numeric,
    actual_qty_total numeric,
    cycle_time_minutes numeric,

    _stg_updated_at timestamptz not null,
    _loaded_at timestamptz not null default now()
);

create table if not exists mart.fact_ib_receipt_line(
    receipt_id uuid not null,
    line_id text not null,

    client_key bigint references mart.dim_client(client_key),
    warehouse_key bigint references mart.dim_warehouse(warehouse_key),
    product_key bigint references mart.dim_product(product_key),

    po_date_key int references mart.dim_date(date_key),

    qty_unit_id text,
    expected_qty numeric,
    actual_qty numeric,

    _stg_updated_at timestamptz not null,
    _loaded_at timestamptz not null default now(),

    primary key (receipt_id,line_id),
    foreign key (receipt_id) references mart.fact_ib_receipt(receipt_id) on delete cascade
);

create table if not exists mart.fact_ob_order(
    order_id uuid primary key,
    so_code text,
    status text,

    client_key bigint references mart.dim_client(client_key),
    warehouse_key bigint references mart.dim_warehouse(warehouse_key),

    created_by_user_key bigint references mart.dim_user(user_key),
    updated_by_user_key bigint references mart.dim_user(user_key),

    customer_key bigint references mart.dim_customer(customer_key),
    shipping_address_key bigint references mart.dim_shipping_address(shipping_address_key),

    expected_delivery_date_key int references mart.dim_date(date_key),
    actual_delivery_date_key int references mart.dim_date(date_key),
    created_date_key int references mart.dim_date(date_key),
    updated_date_key int references mart.dim_date(date_key),   

    expected_delivery_date date,
    actual_delivery_date date,
    created_at timestamptz,
    updated_at timestamptz,

    line_count int not null default 0,
    total_amount numeric,
    actual_amount numeric,
    total_cod_amount numeric,
    total_weight numeric,
    total_volume numeric,
    delivery_delay_days int,
    _stg_updated_at timestamptz not null,
    _loaded_at timestamptz not null default now() 
);

create table if not exists mart.fact_ob_order_line(
    order_id uuid not null,
    line_id text not null,

    client_key bigint references mart.dim_client(client_key),
    warehouse_key bigint references mart.dim_warehouse(warehouse_key),
    product_key bigint references mart.dim_product(product_key),

    qty numeric,

    _stg_updated_at timestamptz not null,
    _loaded_at timestamptz not null default now(),  

    primary key (order_id, line_id),
    foreign key (order_id) references mart.fact_ob_order(order_id) on delete cascade
);

create index if not exists ix_ib_receipt_created_date on mart.fact_ib_receipt(created_date_key);
create index if not exists ix_ib_receipt_wh on mart.fact_ib_receipt(warehouse_key);
create index if not exists ix_ib_receipt_line_product on mart.fact_ib_receipt_line(product_key);

create index if not exists ix_ob_order_created_date on mart.fact_ob_order(created_date_key);
create index if not exists ix_ob_order_wh on mart.fact_ob_order(warehouse_key);
create index if not exists ix_ob_order_line_product on mart.fact_ob_order_line(product_key);

create index if not exists ix_ib_receipt_status on mart.fact_ib_receipt(status);
create index if not exists ix_ob_order_status on mart.fact_ob_order(status);