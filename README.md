# IT Desk 터미널 운영 대시보드

`rich` + `psycopg` 기반의 단일 파일 운영 상황판입니다.
숫자 나열이 아니라, 운영자가 “지금 뭐부터 봐야 하는지”를 한 화면에 보여주는 것을 목표로 설계되었습니다.

## 주요 기능

- **상단 KPI 4카드**: 오늘 신규 / 오늘 완료 / 처리율 / 미처리(미배정·노후 동시 표기)
- **상황 신호(Alert)**: 미배정·노후 미해결·처리 지연·담당자 쏠림·백로그 위험 자동 감지
- **최근 요청 26건(기본)**: 우선순위/노후성 색상 강조, 미처리 우선 정렬, 다중 담당자 표시
- **담당자 워크로드**: admin 전원 표시, rank + 분포 막대, 미배정 하단 고정
- **상태 분포**: 4상태 모두 0건 포함 + 비중 % + 분포 막대
- **7일 추세 sparkline**: 합계/평균/오늘 비교
- **오늘 카테고리**: 모든 카테고리 0건 포함
- **자동 재접속**: DB 끊겨도 지수 백오프로 복구 시도, 마지막 데이터 표시 유지
- **단축키**: `q` 종료 / `r` 즉시 새로고침 / `p` 일시정지 / `c` 컴팩트 / `t` 테마 / `+/-` 주기 조절

## 실행

```powershell
cd F:\01_DEV\work\it-desk\it-desk-dashboard
python -m pip install -r requirements.txt
python dashboard.py
```

## CLI 옵션

```text
--db-url           PostgreSQL 연결 URL (DATABASE_URL 환경변수 우선)
--refresh-sec      자동 갱신 주기(초). 기본 30
--limit-recent     최근 요청 표시 개수 (1~100). 기본 10
--theme            컬러 테마 (dark|mono). 기본 dark
--compact          좁은 터미널용 컴팩트 레이아웃 (카테고리 컬럼 축소 등)
--once             한 번만 조회하고 종료 (스냅샷 출력)
--no-screen        대체 화면 비활성 (디버깅·스크롤백 유지)
--debug            디버그 로그 출력 (stderr)
```

## 추천 실행 예시

```powershell
# 일반 운영 (30초 주기, 최근 26건)
python dashboard.py

# 좁은 터미널/원격 SSH
python dashboard.py --compact --refresh-sec 60

# 흑백 모니터/저자극 환경
python dashboard.py --theme mono

# 1회 스냅샷 (점검·캡처용)
python dashboard.py --once --no-screen

# 디버깅
python dashboard.py --debug --no-screen --refresh-sec 10
```

## 알림 규칙 요약

| 신호 | 조건 |
|------|------|
| 미배정 미처리 | `assignee 없음 AND status in (open, in_progress)` |
| 노후 미해결 | `status in (open, in_progress) AND created_at >= 3일 전` |
| 처리 지연 | `today_new >= 5 AND today_done * 2 < today_new` |
| 담당자 쏠림 | `max_pending / mean_pending >= 2.5 (담당이 있는 admin만)` |
| 백로그 위험 | `pending >= 50 AND today_new - today_done >= 10` |

## 시간대

모든 “오늘/어제/노후” 계산은 `Asia/Seoul (KST)` 기준입니다.
