create table if not exists daily_issue_events (
  issue_id uuid primary key default gen_random_uuid(),
  trade_date date not null,
  section_key text not null check (section_key in ('kospi', 'bio', 'samsung_bio')),
  symbol text not null,
  source_name text not null,
  source_tier smallint not null check (source_tier between 1 and 3),
  title text not null,
  summary text,
  url text not null,
  published_at_kst timestamptz not null,
  language text not null default 'ko',
  topic_tags text[] not null default '{}'::text[],
  sentiment numeric,
  relevance_score numeric not null default 0,
  is_same_day boolean not null default false,
  fetched_at timestamptz not null default now()
);

create unique index if not exists uq_daily_issue_events_source_url_published
  on daily_issue_events(source_name, section_key, url, published_at_kst);

create index if not exists idx_daily_issue_events_date_section
  on daily_issue_events(trade_date, section_key);

create index if not exists idx_daily_issue_events_published
  on daily_issue_events(published_at_kst desc);

create table if not exists daily_section_evidence (
  report_date date not null references daily_reports(report_date) on delete cascade,
  section_key text not null check (section_key in ('kospi', 'bio', 'samsung_bio')),
  rank integer not null check (rank >= 1),
  issue_id uuid references daily_issue_events(issue_id) on delete set null,
  evidence_type text not null check (evidence_type in ('flow', 'macro', 'news', 'disclosure')),
  weight numeric not null default 0,
  reason text not null,
  created_at timestamptz not null default now(),
  primary key (report_date, section_key, rank)
);

create index if not exists idx_daily_section_evidence_issue_id
  on daily_section_evidence(issue_id);

create table if not exists source_fetch_runs (
  run_id uuid primary key default gen_random_uuid(),
  source_name text not null,
  run_at timestamptz not null default now(),
  status text not null check (status in ('success', 'failed', 'partial', 'skipped')),
  http_status integer,
  error_message text,
  metrics_json jsonb not null default '{}'::jsonb
);

alter table daily_issue_events enable row level security;
alter table daily_section_evidence enable row level security;
alter table source_fetch_runs enable row level security;

revoke all on table daily_issue_events, daily_section_evidence, source_fetch_runs from anon, authenticated;

alter table daily_report_sections
  add column if not exists analysis_steps_ko jsonb not null default '[]'::jsonb;

alter table daily_report_sections
  add column if not exists analysis_steps_en jsonb not null default '[]'::jsonb;

alter table daily_report_sections
  add column if not exists evidence_count integer not null default 0;

alter table daily_report_sections
  add column if not exists confidence_score numeric not null default 0;

create or replace view v_latest_report_v2 as
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
  drs.analysis_steps_ko,
  drs.analysis_steps_en,
  drs.chart_key,
  drs.as_of_date,
  drs.evidence_count,
  drs.confidence_score,
  coalesce(
    (
      select jsonb_agg(
        jsonb_build_object(
          'rank', dse.rank,
          'evidence_type', dse.evidence_type,
          'weight', dse.weight,
          'reason', dse.reason,
          'issue_id', die.issue_id,
          'source_name', die.source_name,
          'source_tier', die.source_tier,
          'symbol', die.symbol,
          'title', die.title,
          'summary', die.summary,
          'url', die.url,
          'published_at_kst', die.published_at_kst,
          'language', die.language,
          'topic_tags', die.topic_tags,
          'sentiment', die.sentiment,
          'relevance_score', die.relevance_score
        )
        order by dse.rank
      )
      from daily_section_evidence dse
      left join daily_issue_events die on die.issue_id = dse.issue_id
      where dse.report_date = dr.report_date
        and dse.section_key = drs.section_key
    ),
    '[]'::jsonb
  ) as evidences
from latest
join daily_reports dr on dr.report_date = latest.report_date
join daily_report_sections drs on drs.report_date = dr.report_date
order by drs.section_key;

create or replace view v_report_evidence_v2 as
select
  dse.report_date,
  dse.section_key,
  dse.rank,
  dse.evidence_type,
  dse.weight,
  dse.reason,
  die.issue_id,
  die.source_name,
  die.source_tier,
  die.symbol,
  die.title,
  die.summary,
  die.url,
  die.published_at_kst,
  die.language,
  die.topic_tags,
  die.sentiment,
  die.relevance_score
from daily_section_evidence dse
left join daily_issue_events die on die.issue_id = dse.issue_id;

grant select on v_latest_report_v2 to anon, authenticated;
grant select on v_report_evidence_v2 to anon, authenticated;
