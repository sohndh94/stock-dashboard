# Daily Bio Market Dashboard

Next.js + Supabase 기반의 공개용 일간 바이오 시장 대시보드입니다.

## 구현 범위

- 일간 데이터 저장(Daily OHLCV, 수급, 거시)
- 리포트 섹션 3개 고정
  - 코스피 시장
  - 바이오 산업
  - 삼성바이오로직스
- 한국어/영어 병기 1~2문장 분석
- 2025-07-01=100 기준 비교 차트(BASE100)
- API
  - `GET /api/v1/report/latest?lang=ko|en`
  - `GET /api/v1/charts/{section}?from=2025-07-01&to=YYYY-MM-DD`
  - `GET /api/v1/health/data-lag`

## 기술 스택

- Frontend: Next.js(App Router), TypeScript, ECharts
- DB: Supabase(Postgres)
- Batch: Python + GitHub Actions cron
- Providers: pykrx, yfinance

## 디렉터리

- `app/`: Next.js UI + API route
- `lib/`: 타입, 서버 데이터 접근 로직
- `supabase/sql/001_init.sql`: 초기 스키마 + 공개 뷰 + 권한
- `pipeline/`: 일배치/백필/리포트 생성 파이프라인
- `.github/workflows/`: 스케줄/수동 실행 자동화

## 환경 변수

`/.env.example`를 참고해서 설정합니다.

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_URL`
- `REPORT_START_DATE=2025-07-01`
- `REPORT_CUTOFF_KST=16:00`

## 로컬 실행

```bash
npm install
npm run dev
```

## Supabase 반영 순서

1. `supabase/sql/001_init.sql` 실행
2. `instruments`, `daily_prices`, `daily_reports` 등 테이블 생성 확인
3. `v_latest_report`, `v_chart_series_base100` 뷰 생성 확인

## 파이프라인 실행

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r pipeline/requirements.txt

# 1회 백필
python -m pipeline.jobs.backfill --start 2025-07-01

# 일간 수집
python -m pipeline.jobs.daily_ingest --lookback-days 7

# 리포트 생성
python -m pipeline.jobs.generate_report
```

## 스케줄

- 16:00 KST (`07:00 UTC`): `daily_ingest`
- 16:03 KST (`07:03 UTC`): `generate_report`

## 공개 고지 문구(고정)

- 본 서비스는 공개 무료 데이터 소스를 기반으로 하며 투자 자문이 아닙니다.
- 데이터 지연/정정 가능성이 있습니다. 원거래소 공시를 우선 확인하세요.

## 사용자 체크리스트

1. Supabase 프로젝트 생성 + 키 발급
2. GitHub Secrets 등록 (`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`)
3. Vercel 환경변수 등록
4. SQL 초기 스키마 적용
5. Backfill 워크플로 1회 실행
