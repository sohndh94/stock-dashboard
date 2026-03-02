# Supabase Setup

## 1) SQL 실행

Supabase SQL Editor에서 아래 파일을 실행합니다.

- `supabase/sql/001_init.sql`
- `supabase/sql/002_report_v2.sql`
- `supabase/sql/003_universe_scalable.sql`

## 2) 확인 항목

- 테이블 생성: `instruments`, `instrument_groups`, `instrument_group_members`, `daily_prices`, `daily_company_metrics`, `daily_flows`, `daily_macro`, `daily_reports`, `daily_report_sections`, `job_runs`, `daily_issue_events`, `daily_section_evidence`, `source_fetch_runs`
- 뷰 생성: `v_latest_report`, `v_chart_series_base100`, `v_compare_series_base100`, `v_latest_report_v2`, `v_report_evidence_v2`, `v_compare_universe`, `v_company_latest_snapshot`
- 공개 권한: 뷰는 `anon`/`authenticated` 조회 가능, 원본 테이블은 비공개

## 3) 초기 데이터 적재

로컬 또는 GitHub Actions에서 백필 작업을 1회 실행합니다.

```bash
python -m pipeline.jobs.sync_universe
python -m pipeline.jobs.backfill --start 2025-07-01
python -m pipeline.jobs.daily_issue_ingest
python -m pipeline.jobs.generate_report_v2
```
