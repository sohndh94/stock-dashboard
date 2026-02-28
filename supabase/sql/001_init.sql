create extension if not exists pgcrypto;

create table if not exists instruments (
  instrument_id uuid primary key default gen_random_uuid(),
  symbol text not null unique,
  name text not null,
  category text,
  asset_type text not null check (asset_type in ('stock', 'index', 'macro')),
  market text not null,
  currency text not null,
  is_active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists daily_prices (
  instrument_id uuid not null references instruments(instrument_id),
  trade_date date not null,
  open numeric,
  high numeric,
  low numeric,
  close numeric not null,
  volume numeric,
  source text not null,
  price_date_actual date not null,
  fetched_at timestamptz not null default now(),
  primary key (instrument_id, trade_date)
);

create table if not exists daily_flows (
  target_type text not null check (target_type in ('market', 'stock', 'industry')),
  target_code text not null,
  trade_date date not null,
  investor_type text not null check (investor_type in ('foreign', 'institution', 'retail', 'other')),
  buy_value_krw numeric not null default 0,
  sell_value_krw numeric not null default 0,
  net_value_krw numeric generated always as (buy_value_krw - sell_value_krw) stored,
  source text not null,
  fetched_at timestamptz not null default now(),
  primary key (target_type, target_code, trade_date, investor_type)
);

create table if not exists daily_macro (
  metric_code text not null,
  trade_date date not null,
  value numeric not null,
  unit text not null,
  source text not null,
  fetched_at timestamptz not null default now(),
  primary key (metric_code, trade_date)
);

create table if not exists daily_reports (
  report_date date primary key,
  cutoff_kst time not null default '16:00:00',
  status text not null check (status in ('complete', 'partial')),
  generated_at timestamptz not null default now(),
  notes text
);

create table if not exists daily_report_sections (
  report_date date not null references daily_reports(report_date) on delete cascade,
  section_key text not null check (section_key in ('kospi', 'bio', 'samsung_bio')),
  title_ko text not null,
  title_en text not null,
  analysis_ko text not null,
  analysis_en text not null,
  chart_key text not null check (chart_key in ('kospi', 'bio', 'samsung_bio')),
  as_of_date date not null,
  input_snapshot_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  primary key (report_date, section_key)
);

create table if not exists job_runs (
  run_id uuid primary key default gen_random_uuid(),
  job_name text not null,
  run_at timestamptz not null default now(),
  status text not null check (status in ('success', 'failed', 'partial')),
  error_message text,
  metrics_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_daily_prices_trade_date on daily_prices (trade_date);
create index if not exists idx_daily_report_sections_date_key on daily_report_sections (report_date, section_key);
create index if not exists idx_daily_flows_date on daily_flows (trade_date);
create index if not exists idx_daily_macro_date on daily_macro (trade_date);

alter table instruments enable row level security;
alter table daily_prices enable row level security;
alter table daily_flows enable row level security;
alter table daily_macro enable row level security;
alter table daily_reports enable row level security;
alter table daily_report_sections enable row level security;
alter table job_runs enable row level security;

revoke all on table instruments, daily_prices, daily_flows, daily_macro, daily_reports, daily_report_sections, job_runs from anon, authenticated;

create or replace view v_latest_report as
with latest as (
  select max(report_date) as report_date
  from daily_reports
)
select
  dr.report_date,
  to_char(dr.cutoff_kst, 'HH24:MI') as cutoff_kst,
  dr.status,
  dr.generated_at,
  drs.section_key as section,
  drs.title_ko,
  drs.title_en,
  drs.analysis_ko,
  drs.analysis_en,
  drs.chart_key,
  drs.as_of_date
from latest
join daily_reports dr on dr.report_date = latest.report_date
join daily_report_sections drs on drs.report_date = dr.report_date
order by drs.section_key;

create or replace view v_chart_series_base100 as
with base_symbols as (
  select *
  from (
    values
      ('KOSPI', 'KOSPI', 'KRW'),
      ('KOSPI200_HEALTHCARE', 'KOSPI200 Health Care', 'KRW'),
      ('207940.KS', 'Samsung Biologics', 'KRW'),
      ('068270.KS', 'Celltrion', 'KRW'),
      ('2269.HK', 'Wuxi Biologics', 'HKD'),
      ('LONN.SW', 'Lonza', 'CHF')
  ) as t(symbol, label, currency)
),
spine as (
  select distinct dp.trade_date
  from daily_prices dp
  join instruments i on i.instrument_id = dp.instrument_id
  where i.symbol = 'KOSPI'
    and dp.trade_date >= date '2025-07-01'
),
asof_prices as (
  select
    sp.trade_date,
    bs.symbol,
    bs.label,
    bs.currency,
    (
      select dp.close
      from daily_prices dp
      join instruments i on i.instrument_id = dp.instrument_id
      where i.symbol = bs.symbol
        and dp.trade_date <= sp.trade_date
      order by dp.trade_date desc
      limit 1
    ) as close,
    (
      select dp.trade_date
      from daily_prices dp
      join instruments i on i.instrument_id = dp.instrument_id
      where i.symbol = bs.symbol
        and dp.trade_date <= sp.trade_date
      order by dp.trade_date desc
      limit 1
    ) as price_date_latest
  from spine sp
  cross join base_symbols bs
),
peer_avg as (
  select
    trade_date,
    'BIO_PEER_AVG'::text as symbol,
    'Bio Peer Avg'::text as label,
    'MIXED'::text as currency,
    avg(close) as close,
    max(price_date_latest) as price_date_latest
  from asof_prices
  where symbol in ('207940.KS', '068270.KS', '2269.HK', 'LONN.SW')
    and close is not null
  group by trade_date
),
combined as (
  select trade_date, symbol, label, currency, close, price_date_latest
  from asof_prices
  where close is not null

  union all

  select trade_date, symbol, label, currency, close, price_date_latest
  from peer_avg
),
base_dates as (
  select symbol, min(trade_date) as base_trade_date
  from combined
  where trade_date >= date '2025-07-01'
  group by symbol
),
base_prices as (
  select c.symbol, c.close as base_close
  from combined c
  join base_dates bd on bd.symbol = c.symbol and bd.base_trade_date = c.trade_date
),
normalized as (
  select
    c.trade_date,
    c.symbol,
    c.label,
    c.currency,
    c.price_date_latest,
    round((c.close / nullif(bp.base_close, 0)) * 100, 6) as value
  from combined c
  join base_prices bp on bp.symbol = c.symbol
),
section_map as (
  select *
  from (
    values
      ('kospi', 'KOSPI', 'KOSPI'),
      ('kospi', 'KOSPI200_HEALTHCARE', 'KOSPI200 Health Care'),
      ('kospi', '207940.KS', 'Samsung Biologics'),
      ('kospi', 'BIO_PEER_AVG', 'Bio Peer Avg'),

      ('bio', 'KOSPI200_HEALTHCARE', 'KOSPI200 Health Care'),
      ('bio', 'KOSPI', 'KOSPI'),
      ('bio', '207940.KS', 'Samsung Biologics'),
      ('bio', '068270.KS', 'Celltrion'),
      ('bio', '2269.HK', 'Wuxi Biologics'),
      ('bio', 'LONN.SW', 'Lonza'),

      ('samsung_bio', '207940.KS', 'Samsung Biologics'),
      ('samsung_bio', 'BIO_PEER_AVG', 'Bio Peer Avg'),
      ('samsung_bio', 'KOSPI', 'KOSPI'),
      ('samsung_bio', 'KOSPI200_HEALTHCARE', 'KOSPI200 Health Care')
  ) as t(section_key, symbol, label)
)
select
  sm.section_key,
  n.trade_date,
  n.symbol,
  sm.label,
  n.currency,
  n.price_date_latest,
  n.value
from normalized n
join section_map sm on sm.symbol = n.symbol
where n.trade_date >= date '2025-07-01'
order by sm.section_key, n.trade_date, n.symbol;

grant select on v_latest_report to anon, authenticated;
grant select on v_chart_series_base100 to anon, authenticated;
