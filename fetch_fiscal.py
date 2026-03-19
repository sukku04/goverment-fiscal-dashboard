"""
열린재정 Open API — 기능별 세출 데이터 수집 스크립트
GitHub Actions에서 실행되어 data/ 폴더에 JSON 파일을 저장합니다.

파일 경로 규칙:
  data/{gov}_{type}_{year}.json
  예) data/central_budget_2024.json
      data/central_settlement_2023.json
      data/local_budget_2024.json

수동 실행:
  FISCAL_API_KEY=your_key python scripts/fetch_fiscal.py
  FISCAL_API_KEY=your_key YEARS=2023,2024 GOV_TYPES=central python scripts/fetch_fiscal.py
"""

import os, json, time, requests
from pathlib import Path
from datetime import datetime

# ── 설정 ───────────────────────────────────────────────
API_KEY   = os.environ.get('FISCAL_API_KEY', '')
API_BASE  = 'https://openapi.openfiscaldata.go.kr/'
DATA_DIR  = Path('data')

# 수집 연도: 환경변수 없으면 현재연도 기준 최근 6년
THIS_YEAR = datetime.now().year
DEFAULT_YEARS = list(range(THIS_YEAR - 5, THIS_YEAR + 1))
RAW_YEARS = os.environ.get('YEARS', '')
YEARS = [int(y.strip()) for y in RAW_YEARS.split(',') if y.strip()] if RAW_YEARS else DEFAULT_YEARS

# 수집 대상
RAW_GOV = os.environ.get('GOV_TYPES', 'central,local')
GOV_TYPES = [g.strip() for g in RAW_GOV.split(',')]

# 엔드포인트
ENDPOINTS = {
    'central_budget':     'dFUncBudgetInfo',
    'central_settlement': 'dFUncSettleInfo',
    'local_budget':       'dLFUncBudgetInfo',
    'local_settlement':   'dLFUncSettleInfo',
}

# 분야 코드 (드릴다운용)
FILD_CODES = [
    '010','020','030','040','050','060','070','080',
    '090','100','110','120','130','140','150','160',
]

# ── API 호출 ──────────────────────────────────────────
def fetch_all(endpoint_key, year, extra_params=None, max_pages=10):
    """전체 페이지 수집 (페이지네이션 자동 처리)"""
    ep = ENDPOINTS[endpoint_key]
    all_rows = []

    for page in range(1, max_pages + 1):
        params = {
            'key':     API_KEY,
            'Type':    'json',
            'pIndex':  str(page),
            'pSize':   '300',
            'FSCL_YR': str(year),
        }
        if extra_params:
            params.update(extra_params)

        try:
            resp = requests.get(
                f'{API_BASE}{ep}',
                params=params,
                timeout=30,
                headers={'User-Agent': 'Mozilla/5.0 (fiscal-dashboard/1.0)'}
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f'    ⚠ 요청 실패 (page {page}): {e}')
            break

        # 응답 구조 유연하게 파싱
        root = data.get(ep, data)
        if isinstance(root, dict):
            rows = root.get('list') or root.get('List') or []
            total = int(root.get('totalCount', 0) or root.get('TotalCount', 0) or 0)
        elif isinstance(root, list):
            rows = root
            total = len(root)
        else:
            rows = []
            total = 0

        # 오류 코드 확인
        result = data.get('RESULT', {})
        if result.get('CODE') not in (None, '00', 'INFO-000', 'INFO-001'):
            print(f'    ⚠ API 오류: [{result.get("CODE")}] {result.get("MESSAGE")}')
            break

        all_rows.extend(rows)
        print(f'      page {page}: {len(rows)}건 (누계 {len(all_rows)}/{total})')

        if not rows or len(all_rows) >= total:
            break
        time.sleep(0.3)

    return all_rows


# ── 저장 ─────────────────────────────────────────────
def save(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, separators=(',', ':'))
    print(f'    💾 {path} ({len(rows)}건, {path.stat().st_size/1024:.1f}KB)')


# ── 메인 ─────────────────────────────────────────────
def main():
    if not API_KEY:
        print('❌ FISCAL_API_KEY 환경변수가 없습니다.')
        print('   export FISCAL_API_KEY=your_key')
        raise SystemExit(1)

    print(f'📡 열린재정 Open API 수집 시작')
    print(f'   연도: {YEARS}')
    print(f'   정부: {GOV_TYPES}')
    print(f'   저장 위치: {DATA_DIR}/')
    print()

    stats = {'ok': 0, 'empty': 0, 'error': 0}

    for gov in GOV_TYPES:
        for year in YEARS:
            for dtype in ['budget', 'settlement']:
                ep_key = f'{gov}_{dtype}'
                if ep_key not in ENDPOINTS:
                    continue

                label = f'{gov} / {dtype} / {year}년'
                print(f'  📂 {label}')

                try:
                    rows = fetch_all(ep_key, year)
                    out_path = DATA_DIR / f'{gov}_{dtype}_{year}.json'

                    if rows:
                        save(out_path, rows)
                        stats['ok'] += 1

                        # 드릴다운: 분야별 부문 데이터 수집 (budget만)
                        if dtype == 'budget' and len(rows) > 0:
                            # 첫 행에 FILD_CD가 있으면 분야 수준임 → 부문 수집
                            sample = rows[0]
                            if 'FILD_CD' in sample and 'SECT_CD' not in sample:
                                print(f'      → 분야별 부문 수집...')
                                for fild_cd in FILD_CODES:
                                    drill_rows = fetch_all(
                                        ep_key, year,
                                        extra_params={'FILD_CD': fild_cd}
                                    )
                                    if drill_rows:
                                        drill_path = DATA_DIR / f'{gov}_{dtype}_{year}_{fild_cd}.json'
                                        save(drill_path, drill_rows)
                                    time.sleep(0.2)
                    else:
                        print(f'      ℹ 데이터 없음 (파일 미생성)')
                        stats['empty'] += 1

                except Exception as e:
                    print(f'      ❌ 오류: {e}')
                    stats['error'] += 1

                time.sleep(0.5)

    # 메타데이터
    meta = {
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'years':        YEARS,
        'gov_types':    GOV_TYPES,
        'source':       '열린재정 Open API (openapi.openfiscaldata.go.kr)',
        'stats':        stats,
    }
    save(DATA_DIR / 'meta.json', [meta])
    print()
    print(f'✅ 수집 완료 — 성공: {stats["ok"]}개, 빈데이터: {stats["empty"]}개, 오류: {stats["error"]}개')


if __name__ == '__main__':
    main()
