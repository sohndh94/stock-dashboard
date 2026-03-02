alter table if exists instruments
  add column if not exists name_ko text;

alter table if exists instruments
  add column if not exists provider text not null default 'yfinance';

alter table if exists instruments
  add column if not exists provider_symbol text not null default '';

alter table if exists instruments
  add column if not exists display_order integer not null default 1000;

alter table if exists instruments
  add column if not exists is_compare_default boolean not null default false;

update instruments
set provider_symbol = symbol
where provider_symbol = '' or provider_symbol is null;

create table if not exists instrument_groups (
  group_key text primary key,
  name text not null,
  purpose text,
  is_active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists instrument_group_members (
  group_key text not null references instrument_groups(group_key) on delete cascade,
  instrument_id uuid not null references instruments(instrument_id) on delete cascade,
  weight numeric not null default 1,
  role text not null default 'member',
  created_at timestamptz not null default now(),
  primary key (group_key, instrument_id)
);

create table if not exists daily_company_metrics (
  instrument_id uuid not null references instruments(instrument_id) on delete cascade,
  trade_date date not null,
  market_cap numeric,
  shares_outstanding numeric,
  source text not null,
  fetched_at timestamptz not null default now(),
  primary key (instrument_id, trade_date)
);

create index if not exists idx_igm_instrument on instrument_group_members(instrument_id);
create index if not exists idx_dcm_trade_date on daily_company_metrics(trade_date);

alter table instrument_groups enable row level security;
alter table instrument_group_members enable row level security;
alter table daily_company_metrics enable row level security;

revoke all on table instrument_groups, instrument_group_members, daily_company_metrics from anon, authenticated;

create or replace view v_compare_universe as
select
  i.symbol,
  i.name,
  i.name_ko,
  i.category,
  i.asset_type,
  i.market,
  i.currency,
  i.provider,
  i.provider_symbol,
  i.display_order,
  i.is_compare_default,
  i.is_active,
  igm.weight,
  igm.role
from instrument_group_members igm
join instrument_groups ig on ig.group_key = igm.group_key
join instruments i on i.instrument_id = igm.instrument_id
where ig.group_key = 'compare'
  and ig.is_active = true
  and i.is_active = true
order by i.display_order, i.symbol;

create or replace view v_company_latest_snapshot as
with ranked_price as (
  select
    i.instrument_id,
    i.symbol,
    i.name,
    i.name_ko,
    i.market,
    i.currency,
    dp.trade_date,
    dp.close,
    dp.price_date_actual,
    lag(dp.close) over (partition by i.instrument_id order by dp.trade_date) as prev_close,
    row_number() over (partition by i.instrument_id order by dp.trade_date desc) as rn
  from instruments i
  left join daily_prices dp on dp.instrument_id = i.instrument_id
  where i.is_active = true
),
latest_metric as (
  select distinct on (dcm.instrument_id)
    dcm.instrument_id,
    dcm.trade_date as metric_trade_date,
    dcm.market_cap,
    dcm.shares_outstanding,
    dcm.source as metric_source
  from daily_company_metrics dcm
  order by dcm.instrument_id, dcm.trade_date desc
)
select
  rp.symbol,
  rp.name,
  rp.name_ko,
  rp.market,
  rp.currency,
  rp.trade_date,
  rp.price_date_actual,
  rp.close,
  rp.prev_close,
  case
    when rp.prev_close is null or rp.prev_close = 0 then null
    else round(((rp.close - rp.prev_close) / rp.prev_close) * 100, 6)
  end as day_change_pct,
  lm.metric_trade_date,
  lm.market_cap,
  lm.shares_outstanding,
  lm.metric_source
from ranked_price rp
left join latest_metric lm on lm.instrument_id = rp.instrument_id
where rp.rn = 1;

create or replace view v_chart_series_base100 as
with section_groups as (
  select *
  from (
    values
      ('kospi'::text, 'chart_kospi'::text),
      ('bio'::text, 'chart_bio'::text),
      ('samsung_bio'::text, 'chart_samsung_bio'::text)
  ) as t(section_key, group_key)
),
section_members as (
  select
    sg.section_key,
    i.symbol,
    i.name as label,
    i.currency
  from section_groups sg
  join instrument_group_members gm on gm.group_key = sg.group_key
  join instruments i on i.instrument_id = gm.instrument_id
  where i.is_active = true
),
required_symbols as (
  select distinct
    i.symbol,
    i.name as label,
    i.currency
  from instrument_group_members gm
  join instruments i on i.instrument_id = gm.instrument_id
  where gm.group_key in ('chart_kospi', 'chart_bio', 'chart_samsung_bio', 'bio_peer_core')
    and i.is_active = true
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
    rs.symbol,
    rs.label,
    rs.currency,
    (
      select dp.close
      from daily_prices dp
      join instruments i on i.instrument_id = dp.instrument_id
      where i.symbol = rs.symbol
        and dp.trade_date <= sp.trade_date
      order by dp.trade_date desc
      limit 1
    ) as close,
    (
      select dp.trade_date
      from daily_prices dp
      join instruments i on i.instrument_id = dp.instrument_id
      where i.symbol = rs.symbol
        and dp.trade_date <= sp.trade_date
      order by dp.trade_date desc
      limit 1
    ) as price_date_latest
  from spine sp
  cross join required_symbols rs
),
peer_symbols as (
  select i.symbol
  from instrument_group_members gm
  join instruments i on i.instrument_id = gm.instrument_id
  where gm.group_key = 'bio_peer_core'
    and i.is_active = true
),
peer_avg as (
  select
    ap.trade_date,
    'BIO_PEER_AVG'::text as symbol,
    'Bio Peer Avg'::text as label,
    'MIXED'::text as currency,
    avg(ap.close) as close,
    max(ap.price_date_latest) as price_date_latest
  from asof_prices ap
  join peer_symbols ps on ps.symbol = ap.symbol
  where ap.close is not null
  group by ap.trade_date
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
  group by symbol
),
base_prices as (
  select c.symbol, c.close as base_close
  from combined c
  join base_dates bd
    on bd.symbol = c.symbol
   and bd.base_trade_date = c.trade_date
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
  select section_key, symbol, label
  from section_members
  union all
  select 'kospi'::text, 'BIO_PEER_AVG'::text, 'Bio Peer Avg'::text
  union all
  select 'samsung_bio'::text, 'BIO_PEER_AVG'::text, 'Bio Peer Avg'::text
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

create or replace view v_compare_series_base100 as
with compare_symbols as (
  select
    i.symbol,
    i.name as label,
    i.currency
  from instrument_group_members gm
  join instruments i on i.instrument_id = gm.instrument_id
  where gm.group_key = 'compare'
    and i.is_active = true
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
    cs.symbol,
    cs.label,
    cs.currency,
    (
      select dp.close
      from daily_prices dp
      join instruments i on i.instrument_id = dp.instrument_id
      where i.symbol = cs.symbol
        and dp.trade_date <= sp.trade_date
      order by dp.trade_date desc
      limit 1
    ) as close,
    (
      select dp.trade_date
      from daily_prices dp
      join instruments i on i.instrument_id = dp.instrument_id
      where i.symbol = cs.symbol
        and dp.trade_date <= sp.trade_date
      order by dp.trade_date desc
      limit 1
    ) as price_date_latest
  from spine sp
  cross join compare_symbols cs
),
base_dates as (
  select symbol, min(trade_date) as base_trade_date
  from asof_prices
  where close is not null
  group by symbol
),
base_prices as (
  select ap.symbol, ap.close as base_close
  from asof_prices ap
  join base_dates bd
    on bd.symbol = ap.symbol
   and bd.base_trade_date = ap.trade_date
),
normalized as (
  select
    ap.trade_date,
    ap.symbol,
    ap.label,
    ap.currency,
    ap.price_date_latest,
    round((ap.close / nullif(bp.base_close, 0)) * 100, 6) as value
  from asof_prices ap
  join base_prices bp on bp.symbol = ap.symbol
  where ap.close is not null
)
select
  trade_date,
  symbol,
  label,
  currency,
  price_date_latest,
  value
from normalized
where trade_date >= date '2025-07-01'
order by trade_date, symbol;

grant select on v_compare_universe to anon, authenticated;
grant select on v_company_latest_snapshot to anon, authenticated;
grant select on v_compare_series_base100 to anon, authenticated;
grant select on v_chart_series_base100 to anon, authenticated;
