"""IT Desk 터미널 운영 대시보드 (Rich 기반).

운영 의사결정에 도움이 되는 상황판 형태의 단일 파일 대시보드.

레이어 구성
============
1) Config / Theme / Constants
2) Models (dataclass)
3) URL Utils
4) DB Repository (Query Layer)
5) Analytics (Insights 계산)
6) UI Renderers
7) KeyListener (선택적, OS별 비차단 입력)
8) App Controller (Live 루프 + 자동 재접속)
9) CLI
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import psycopg
from psycopg.rows import dict_row
from rich import box
from rich.align import Align
from rich.columns import Columns
from rich.console import Console, Group, RenderableType
from rich.layout import Layout
from rich.live import Live
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text


# ============================================================================
# 1) 설정 / 상수 / 테마
# ============================================================================

DEFAULT_DB_URL = (
    "postgresql+psycopg://kdis_ticket:kdis3299%21@10.1.10.6:5432/"
    "kdis_ticket?options=-csearch_path%3Dticket"
)
DEFAULT_REFRESH_SEC = 30
# 하단(작업유형·카테고리) 전체 표시를 위해 기본은 짧게; --limit-recent로 늘릴 수 있음
DEFAULT_RECENT_LIMIT = 8
# 요청현황 패널: 데이터 행 + 타이틀·테이블 헤더·테두리 대략치
RECENT_PANEL_OVERHEAD = 4
# 담당자 워크로드: 데이터 행 + 패널 타이틀·테이블 헤더·테두리(담당자 열은 1행 고정)
WORKLOAD_PANEL_OVERHEAD = 6
NEW_ALERT_DURATION_SEC = 4.0
NEW_ALERT_BLINK_SEC = 0.45
LIVE_SPINNER_FRAMES = ("|", "/", "-", "\\")
LIVE_SPINNER_STEP_SEC = 0.12
LIVE_BLINK_SEC = 0.5

# 운영 임계치
STALE_THRESHOLD_DAYS = 3       # N일 이상 미해결이면 노후로 간주
RECENT_TITLE_MAX = 36          # 최근 요청 제목 표시 최대 길이
RECENT_CATEGORY_MAX = 18       # 최근 요청 카테고리 최대 길이(줄바꿈 방지)

LOGGER = logging.getLogger("it_desk_dashboard")

STATUS_LABEL: dict[str, str] = {
    "open": "대기",
    "in_progress": "진행",
    "resolved": "완료",
    "closed": "사업검토",
}

PRIORITY_LABEL: dict[str, str] = {
    "urgent": "긴급",
    "high": "높음",
    "medium": "보통",
    "low": "낮음",
}

PRIORITY_STYLE: dict[str, str] = {
    "urgent": "bold red",
    "high": "bold yellow",
    "medium": "white",
    "low": "grey50",
}


@dataclass(frozen=True)
class Theme:
    """일관된 색상 팔레트."""
    name: str
    accent: str
    info: str
    muted: str
    ok: str
    warn: str
    danger: str
    status_open: str
    status_in_progress: str
    status_resolved: str
    status_closed: str

    def status_color(self, key: str) -> str:
        return {
            "open": self.status_open,
            "in_progress": self.status_in_progress,
            "resolved": self.status_resolved,
            "closed": self.status_closed,
        }.get(key, self.muted)

    def severity_color(self, level: str) -> str:
        return {
            "ok": self.ok,
            "info": self.info,
            "warn": self.warn,
            "danger": self.danger,
        }.get(level, self.info)


THEMES: dict[str, Theme] = {
    "dark": Theme(
        name="dark",
        accent="bright_cyan",
        info="cyan",
        muted="grey50",
        ok="green",
        warn="yellow",
        danger="red",
        status_open="yellow",
        status_in_progress="cyan",
        status_resolved="green",
        status_closed="magenta",
    ),
    "mono": Theme(
        name="mono",
        accent="bright_white",
        info="white",
        muted="grey50",
        ok="bright_white",
        warn="bright_white",
        danger="bright_white",
        status_open="bright_white",
        status_in_progress="white",
        status_resolved="white",
        status_closed="grey50",
    ),
}


# ============================================================================
# 2) 데이터 모델
# ============================================================================

@dataclass
class SummaryMetrics:
    total_tickets: int = 0
    pending_tickets: int = 0
    today_new: int = 0
    yesterday_new: int = 0
    today_done: int = 0
    yesterday_done: int = 0
    unassigned_pending: int = 0   # 담당자 없는 미처리
    stale_open: int = 0           # N일 이상 묵힌 미해결


@dataclass
class AssigneeWorkload:
    assignee: str
    pending_count: int
    done_today_count: int
    done_week_count: int
    done_total_count: int
    is_unassigned: bool = False


@dataclass
class CategoryToday:
    category: str
    today_count: int
    week_count: int
    total_count: int


@dataclass
class WorkTypeToday:
    work_type: str
    today_count: int
    week_count: int
    total_count: int


@dataclass
class RecentRequest:
    id: int
    title: str
    category: str
    requester: str
    assignee: str
    is_unassigned: bool
    status: str
    priority: str
    created_at_kst: str
    age_hours: float


@dataclass
class TrendPoint:
    label: str
    count: int


@dataclass
class Alert:
    """상단 경고 영역 항목."""
    level: str  # ok / info / warn / danger
    title: str
    detail: str


@dataclass
class Insights:
    """원시 집계로부터 계산된 운영 신호."""
    throughput_pct: float | None = None
    new_trend_pct: float | None = None
    done_trend_pct: float | None = None
    backlog_level: str = "정상"     # 정상 / 관리 / 주의 / 위험
    backlog_severity: str = "ok"     # 색상 매핑
    top_busy: AssigneeWorkload | None = None
    concentration_ratio: float | None = None
    alerts: list[Alert] = field(default_factory=list)


@dataclass
class DashboardData:
    summary: SummaryMetrics
    by_assignee_workload: list[AssigneeWorkload]
    by_category_today: list[CategoryToday]
    by_work_type_today: list[WorkTypeToday]
    recent_requests: list[RecentRequest]
    trend_7d: list[TrendPoint]
    insights: Insights
    refreshed_at: datetime


@dataclass
class ViewOptions:
    recent_show_category: bool
    recent_show_requester: bool
    recent_show_created: bool
    bottom_show_week: bool
    workload_show_week: bool
    workload_show_total: bool


def derive_view_options(term_width: int, term_height: int, force_compact: bool) -> ViewOptions:
    """터미널 문자 셀 크기에 따라 컬럼/행 가시 정책을 결정."""
    compact = force_compact or term_width < 160 or term_height < 46
    very_compact = term_width < 140 or term_height < 40

    return ViewOptions(
        recent_show_category=(not very_compact),
        recent_show_requester=(not very_compact),
        recent_show_created=(not compact),
        bottom_show_week=(not very_compact),
        workload_show_week=(not very_compact),
        workload_show_total=(term_width >= 170 and not compact),
    )


# ============================================================================
# 3) URL 유틸
# ============================================================================

def normalize_database_url(database_url: str) -> str:
    """SQLAlchemy 형식(`postgresql+psycopg://...`)을 psycopg 표준 URL로 변환."""
    if database_url.startswith("postgresql+psycopg://"):
        database_url = "postgresql://" + database_url[len("postgresql+psycopg://"):]
    return database_url


def parse_search_path_from_url(database_url: str) -> str | None:
    """`options=-csearch_path=...`에서 schema 이름만 뽑아 표시용으로 반환."""
    parsed = urlparse(database_url)
    query = parse_qs(parsed.query)
    raw_options = query.get("options", [])
    if not raw_options:
        return None
    decoded = unquote(raw_options[0])
    marker = "-csearch_path="
    if marker not in decoded:
        return None
    return decoded.split(marker, 1)[1].strip() or None


# ============================================================================
# 4) DB Repository (Query Layer)
# ============================================================================

class DashboardRepository:
    """대시보드용 집계 쿼리 모음. 한 사이클에 여러 쿼리를 실행한다."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn

    def fetch(self, recent_limit: int) -> DashboardData:
        summary = self._fetch_summary()
        workload = self._fetch_assignee_workload()
        category_today = self._fetch_category_today()
        work_type_today = self._fetch_work_type_today()
        recent = self._fetch_recent_requests(recent_limit)
        trend = self._fetch_trend_7d()
        return DashboardData(
            summary=summary,
            by_assignee_workload=workload,
            by_category_today=category_today,
            by_work_type_today=work_type_today,
            recent_requests=recent,
            trend_7d=trend,
            insights=Insights(),  # compute_insights에서 채움
            refreshed_at=datetime.now(),
        )

    # --- 요약 (단일 쿼리에 운영 지표까지 묶음) ---
    def _fetch_summary(self) -> SummaryMetrics:
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH bounds AS (
                    SELECT
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AS kst_today,
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') - interval '1 day' AS kst_yesterday,
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') - make_interval(days => %s) AS stale_threshold
                )
                SELECT
                    (SELECT count(*) FROM tickets) AS total_tickets,
                    (SELECT count(*) FROM tickets WHERE status IN ('open','in_progress')) AS pending_tickets,
                    (SELECT count(*) FROM tickets t, bounds b
                       WHERE (t.created_at AT TIME ZONE 'Asia/Seoul') >= b.kst_today) AS today_new,
                    (SELECT count(*) FROM tickets t, bounds b
                       WHERE (t.created_at AT TIME ZONE 'Asia/Seoul') >= b.kst_yesterday
                         AND (t.created_at AT TIME ZONE 'Asia/Seoul') < b.kst_today) AS yesterday_new,
                    (SELECT count(*) FROM tickets t, bounds b
                       WHERE t.status IN ('resolved','closed')
                         AND (t.updated_at AT TIME ZONE 'Asia/Seoul') >= b.kst_today) AS today_done,
                    (SELECT count(*) FROM tickets t, bounds b
                       WHERE t.status IN ('resolved','closed')
                         AND (t.updated_at AT TIME ZONE 'Asia/Seoul') >= b.kst_yesterday
                         AND (t.updated_at AT TIME ZONE 'Asia/Seoul') < b.kst_today) AS yesterday_done,
                    (SELECT count(*) FROM tickets t
                       WHERE t.status IN ('open','in_progress')
                         AND NOT EXISTS (SELECT 1 FROM ticket_assignees ta WHERE ta.ticket_id = t.id)
                    ) AS unassigned_pending,
                    (SELECT count(*) FROM tickets t, bounds b
                       WHERE t.status IN ('open','in_progress')
                         AND (t.created_at AT TIME ZONE 'Asia/Seoul') < b.stale_threshold
                    ) AS stale_open
                """,
                (STALE_THRESHOLD_DAYS,),
            )
            row = cur.fetchone() or {}
        return SummaryMetrics(
            total_tickets=int(row.get("total_tickets") or 0),
            pending_tickets=int(row.get("pending_tickets") or 0),
            today_new=int(row.get("today_new") or 0),
            yesterday_new=int(row.get("yesterday_new") or 0),
            today_done=int(row.get("today_done") or 0),
            yesterday_done=int(row.get("yesterday_done") or 0),
            unassigned_pending=int(row.get("unassigned_pending") or 0),
            stale_open=int(row.get("stale_open") or 0),
        )

    def _fetch_assignee_workload(self) -> list[AssigneeWorkload]:
        """admin 전원 + 미배정. 다중 담당자(`ticket_assignees`) 기준."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH bounds AS (
                    SELECT
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AS kst_today,
                        date_trunc('week', now() AT TIME ZONE 'Asia/Seoul') AS kst_week_start
                ),
                admins AS (
                    SELECT emp_no, COALESCE(NULLIF(kor_name,''), emp_no) AS assignee
                    FROM users
                    WHERE role = 'admin'
                ),
                pending AS (
                    SELECT ta.emp_no, count(*)::int AS pending_count
                    FROM tickets t
                    JOIN ticket_assignees ta ON ta.ticket_id = t.id
                    WHERE t.status IN ('open','in_progress')
                    GROUP BY ta.emp_no
                ),
                done_today AS (
                    SELECT ta.emp_no, count(*)::int AS done_today_count
                    FROM tickets t
                    JOIN ticket_assignees ta ON ta.ticket_id = t.id
                    CROSS JOIN bounds b
                    WHERE t.status IN ('resolved','closed')
                      AND (t.updated_at AT TIME ZONE 'Asia/Seoul') >= b.kst_today
                    GROUP BY ta.emp_no
                ),
                done_week AS (
                    SELECT ta.emp_no, count(*)::int AS done_week_count
                    FROM tickets t
                    JOIN ticket_assignees ta ON ta.ticket_id = t.id
                    CROSS JOIN bounds b
                    WHERE t.status IN ('resolved','closed')
                      AND (t.updated_at AT TIME ZONE 'Asia/Seoul') >= b.kst_week_start
                    GROUP BY ta.emp_no
                ),
                done_total AS (
                    SELECT ta.emp_no, count(*)::int AS done_total_count
                    FROM tickets t
                    JOIN ticket_assignees ta ON ta.ticket_id = t.id
                    WHERE t.status IN ('resolved','closed')
                    GROUP BY ta.emp_no
                ),
                unassigned AS (
                    SELECT
                        '미배정'::text AS assignee,
                        (SELECT count(*)::int FROM tickets t
                            WHERE t.status IN ('open','in_progress')
                              AND NOT EXISTS (SELECT 1 FROM ticket_assignees ta WHERE ta.ticket_id = t.id)
                        ) AS pending_count,
                        (SELECT count(*)::int FROM tickets t
                            CROSS JOIN bounds b
                            WHERE t.status IN ('resolved','closed')
                              AND (t.updated_at AT TIME ZONE 'Asia/Seoul') >= b.kst_today
                              AND NOT EXISTS (SELECT 1 FROM ticket_assignees ta WHERE ta.ticket_id = t.id)
                        ) AS done_today_count,
                        (SELECT count(*)::int FROM tickets t
                            CROSS JOIN bounds b
                            WHERE t.status IN ('resolved','closed')
                              AND (t.updated_at AT TIME ZONE 'Asia/Seoul') >= b.kst_week_start
                              AND NOT EXISTS (SELECT 1 FROM ticket_assignees ta WHERE ta.ticket_id = t.id)
                        ) AS done_week_count,
                        (SELECT count(*)::int FROM tickets t
                            WHERE t.status IN ('resolved','closed')
                              AND NOT EXISTS (SELECT 1 FROM ticket_assignees ta WHERE ta.ticket_id = t.id)
                        ) AS done_total_count
                ),
                joined AS (
                    SELECT
                        a.assignee,
                        COALESCE(p.pending_count, 0)::int AS pending_count,
                        COALESCE(d.done_today_count, 0)::int AS done_today_count,
                        COALESCE(w.done_week_count, 0)::int AS done_week_count,
                        COALESCE(tt.done_total_count, 0)::int AS done_total_count,
                        false AS is_unassigned
                    FROM admins a
                    LEFT JOIN pending p ON p.emp_no = a.emp_no
                    LEFT JOIN done_today d ON d.emp_no = a.emp_no
                    LEFT JOIN done_week w ON w.emp_no = a.emp_no
                    LEFT JOIN done_total tt ON tt.emp_no = a.emp_no
                    UNION ALL
                    SELECT assignee, pending_count, done_today_count, done_week_count, done_total_count, true AS is_unassigned
                    FROM unassigned
                )
                SELECT *
                FROM joined
                WHERE
                    is_unassigned = false
                    AND assignee <> '관리자'
                ORDER BY
                    pending_count DESC,
                    done_today_count DESC,
                    assignee ASC
                """
            )
            rows = cur.fetchall()
        return [
            AssigneeWorkload(
                assignee=str(r["assignee"]),
                pending_count=int(r["pending_count"] or 0),
                done_today_count=int(r["done_today_count"] or 0),
                done_week_count=int(r["done_week_count"] or 0),
                done_total_count=int(r["done_total_count"] or 0),
                is_unassigned=bool(r["is_unassigned"]),
            )
            for r in rows
        ]

    def _fetch_category_today(self) -> list[CategoryToday]:
        """카테고리별 오늘/이번주/총 건수. ticket_categories 기준, 0건 포함(미분류 제외)."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH bounds AS (
                    SELECT
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AS kst_today,
                        date_trunc('week', now() AT TIME ZONE 'Asia/Seoul') AS kst_week_start
                ),
                today_category AS (
                    SELECT t.category_id, count(*)::int AS count
                    FROM tickets t
                    CROSS JOIN bounds b
                    WHERE (t.created_at AT TIME ZONE 'Asia/Seoul') >= b.kst_today
                    GROUP BY t.category_id
                ),
                week_category AS (
                    SELECT t.category_id, count(*)::int AS count
                    FROM tickets t
                    CROSS JOIN bounds b
                    WHERE (t.created_at AT TIME ZONE 'Asia/Seoul') >= b.kst_week_start
                    GROUP BY t.category_id
                ),
                total_category AS (
                    SELECT t.category_id, count(*)::int AS count
                    FROM tickets t
                    GROUP BY t.category_id
                )
                SELECT category, today_count, week_count, total_count
                FROM (
                    SELECT
                        COALESCE(NULLIF(c.name,''), '카테고리#' || c.id::text) AS category,
                        COALESCE(tc.count, 0)::int AS today_count,
                        COALESCE(wc.count, 0)::int AS week_count,
                        COALESCE(tt.count, 0)::int AS total_count
                    FROM ticket_categories c
                    LEFT JOIN today_category tc ON tc.category_id = c.id
                    LEFT JOIN week_category wc ON wc.category_id = c.id
                    LEFT JOIN total_category tt ON tt.category_id = c.id
                ) x
                ORDER BY total_count DESC, category ASC
                """
            )
            rows = cur.fetchall()
        return [
            CategoryToday(
                category=str(r["category"]),
                today_count=int(r["today_count"] or 0),
                week_count=int(r["week_count"] or 0),
                total_count=int(r["total_count"] or 0),
            )
            for r in rows
        ]

    def _fetch_work_type_today(self) -> list[WorkTypeToday]:
        """작업유형별 오늘/이번주/총 건수(한글 라벨 + 0건 포함)."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH bounds AS (
                    SELECT
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AS kst_today,
                        date_trunc('week', now() AT TIME ZONE 'Asia/Seoul') AS kst_week_start
                ),
                work_type_base AS (
                    SELECT *
                    FROM (VALUES
                        (1, 'incident', '장애'),
                        (2, 'request', '요청'),
                        (3, 'change', '변경'),
                        (4, 'permission', '권한'),
                        (5, 'security', '보안'),
                        (6, 'inquiry', '문의')
                    ) AS v(ord, work_type_key, work_type_label)
                ),
                normalized_tickets AS (
                    SELECT
                        CASE
                            WHEN t.work_type IN ('incident', 'request', 'change', 'permission', 'security', 'inquiry')
                                THEN t.work_type
                            WHEN t.work_type IN ('other', 'maintenance', 'project') OR t.work_type IS NULL OR t.work_type = ''
                                THEN 'inquiry'
                            ELSE 'inquiry'
                        END AS work_type_key,
                        (t.created_at AT TIME ZONE 'Asia/Seoul') AS created_kst
                    FROM tickets t
                ),
                today_work_type AS (
                    SELECT nt.work_type_key, count(*)::int AS count
                    FROM normalized_tickets nt
                    CROSS JOIN bounds b
                    WHERE nt.created_kst >= b.kst_today
                    GROUP BY nt.work_type_key
                ),
                week_work_type AS (
                    SELECT nt.work_type_key, count(*)::int AS count
                    FROM normalized_tickets nt
                    CROSS JOIN bounds b
                    WHERE nt.created_kst >= b.kst_week_start
                    GROUP BY nt.work_type_key
                ),
                total_work_type AS (
                    SELECT nt.work_type_key, count(*)::int AS count
                    FROM normalized_tickets nt
                    GROUP BY nt.work_type_key
                )
                SELECT
                    wb.work_type_label AS work_type,
                    COALESCE(twt.count, 0)::int AS today_count,
                    COALESCE(wwt.count, 0)::int AS week_count,
                    COALESCE(tt.count, 0)::int AS total_count
                FROM work_type_base wb
                LEFT JOIN today_work_type twt ON twt.work_type_key = wb.work_type_key
                LEFT JOIN week_work_type wwt ON wwt.work_type_key = wb.work_type_key
                LEFT JOIN total_work_type tt ON tt.work_type_key = wb.work_type_key
                ORDER BY COALESCE(tt.count, 0) DESC, wb.ord ASC
                """
            )
            rows = cur.fetchall()
        return [
            WorkTypeToday(
                work_type=str(r["work_type"]),
                today_count=int(r["today_count"] or 0),
                week_count=int(r["week_count"] or 0),
                total_count=int(r["total_count"] or 0),
            )
            for r in rows
        ]

    def _fetch_recent_requests(self, limit: int) -> list[RecentRequest]:
        """다중 담당자 집계 + 우선순위/노후성 표기를 위해 priority, age_hours 포함."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH bounds AS (
                    SELECT date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AS kst_today
                ),
                assignee_names AS (
                    SELECT
                        ta.ticket_id,
                        string_agg(
                            DISTINCT COALESCE(NULLIF(u.kor_name,''), ta.emp_no),
                            ', ' ORDER BY COALESCE(NULLIF(u.kor_name,''), ta.emp_no)
                        ) AS assignees
                    FROM ticket_assignees ta
                    LEFT JOIN users u ON u.emp_no = ta.emp_no
                    GROUP BY ta.ticket_id
                )
                SELECT
                    t.id,
                    t.title,
                    t.priority,
                    t.status,
                    COALESCE(NULLIF(c.name,''), '미분류') AS category,
                    COALESCE(NULLIF(ru.kor_name,''), t.requester_emp_no) AS requester,
                    COALESCE(an.assignees, NULLIF(au.kor_name,''), t.assignee_emp_no) AS assignee,
                    (an.ticket_id IS NULL AND t.assignee_emp_no IS NULL) AS is_unassigned,
                    to_char((t.created_at AT TIME ZONE 'Asia/Seoul'), 'MM-DD HH24:MI') AS created_at_kst,
                    EXTRACT(EPOCH FROM (now() - t.created_at)) / 3600.0 AS age_hours
                FROM tickets t
                LEFT JOIN ticket_categories c ON c.id = t.category_id
                LEFT JOIN users ru ON ru.emp_no = t.requester_emp_no
                LEFT JOIN users au ON au.emp_no = t.assignee_emp_no
                LEFT JOIN assignee_names an ON an.ticket_id = t.id
                CROSS JOIN bounds b
                WHERE
                    t.status IN ('open', 'in_progress')
                    OR (t.created_at AT TIME ZONE 'Asia/Seoul') >= b.kst_today
                ORDER BY
                    CASE WHEN t.status IN ('open','in_progress') THEN 0 ELSE 1 END,
                    CASE
                        WHEN t.status = 'open' THEN 0
                        WHEN t.status = 'in_progress' THEN 1
                        WHEN t.status = 'resolved' THEN 2
                        WHEN t.status = 'closed' THEN 3
                        ELSE 4
                    END,
                    t.created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [
            RecentRequest(
                id=int(r["id"]),
                title=str(r["title"] or ""),
                category=str(r["category"] or "-"),
                requester=str(r["requester"] or "-"),
                assignee=str(r["assignee"] or "미배정"),
                is_unassigned=bool(r["is_unassigned"]),
                status=str(r["status"] or ""),
                priority=str(r["priority"] or "medium"),
                created_at_kst=str(r["created_at_kst"] or ""),
                age_hours=float(r["age_hours"] or 0.0),
            )
            for r in rows
        ]

    def _fetch_trend_7d(self) -> list[TrendPoint]:
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH day_series AS (
                    SELECT generate_series(
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') - interval '4 days',
                        date_trunc('day', now() AT TIME ZONE 'Asia/Seoul'),
                        interval '1 day'
                    )::date AS day_kst
                ),
                daily_count AS (
                    SELECT
                        (created_at AT TIME ZONE 'Asia/Seoul')::date AS day_kst,
                        count(*)::int AS count
                    FROM tickets
                    WHERE (created_at AT TIME ZONE 'Asia/Seoul')
                          >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') - interval '4 days'
                    GROUP BY 1
                )
                SELECT to_char(s.day_kst, 'MM/DD') AS label,
                       COALESCE(d.count, 0)::int AS count
                FROM day_series s
                LEFT JOIN daily_count d ON d.day_kst = s.day_kst
                ORDER BY s.day_kst
                """
            )
            rows = cur.fetchall()
        return [TrendPoint(label=str(r["label"]), count=int(r["count"] or 0)) for r in rows]


# ============================================================================
# 5) Analytics — 운영 신호 계산
# ============================================================================

def _pct_diff(today: int, yesterday: int) -> float | None:
    if today == 0 and yesterday == 0:
        return 0.0
    if yesterday == 0:
        return None
    return (today - yesterday) / yesterday * 100.0


def compute_insights(data: DashboardData) -> Insights:
    """원시 집계로부터 운영 의사결정용 신호를 계산."""
    s = data.summary
    workload = data.by_assignee_workload

    throughput = None
    if s.today_new > 0:
        throughput = min(999.9, s.today_done / s.today_new * 100.0)

    # 백로그 압력 등급
    net_inflow = s.today_new - s.today_done
    if s.pending_tickets >= 50 and net_inflow >= 10:
        backlog_level, severity = "위험", "danger"
    elif s.pending_tickets >= 30 or net_inflow >= 5 or s.stale_open >= 5:
        backlog_level, severity = "주의", "warn"
    elif s.pending_tickets > 0:
        backlog_level, severity = "관리", "info"
    else:
        backlog_level, severity = "정상", "ok"

    # 담당자 쏠림 (admin 중 미처리>0 만 평가)
    assigned = [w for w in workload if not w.is_unassigned and w.pending_count > 0]
    top_busy: AssigneeWorkload | None = None
    concentration_ratio: float | None = None
    if assigned:
        top_busy = max(assigned, key=lambda w: w.pending_count)
        avg = sum(w.pending_count for w in assigned) / len(assigned)
        concentration_ratio = top_busy.pending_count / avg if avg > 0 else None

    # 알림 컴파일
    alerts: list[Alert] = []
    if s.unassigned_pending > 0:
        alerts.append(Alert(
            level="warn",
            title=f"미배정 미처리 {s.unassigned_pending}건",
            detail="담당자가 지정되지 않은 미처리 티켓이 있습니다. 우선 배정이 필요합니다.",
        ))
    if s.stale_open > 0:
        alerts.append(Alert(
            level="warn",
            title=f"노후 미해결 {s.stale_open}건",
            detail=f"{STALE_THRESHOLD_DAYS}일 이상 처리되지 않은 미해결 요청이 있습니다.",
        ))
    if s.today_new >= 5 and s.today_done * 2 < s.today_new:
        alerts.append(Alert(
            level="warn",
            title="처리 지연 신호",
            detail=f"오늘 신규 {s.today_new}건 대비 완료 {s.today_done}건 — 처리율이 낮습니다.",
        ))
    if (concentration_ratio is not None and concentration_ratio >= 2.5
            and top_busy is not None and len(assigned) >= 2):
        alerts.append(Alert(
            level="info",
            title="담당자 쏠림",
            detail=f"{top_busy.assignee} 담당자에게 미처리가 평균의 {concentration_ratio:.1f}배 집중되어 있습니다.",
        ))
    if backlog_level == "위험":
        alerts.append(Alert(
            level="danger",
            title="백로그 위험 수준",
            detail=f"미처리 {s.pending_tickets}건 · 순유입 +{net_inflow}건. 즉시 분배·처리 권고.",
        ))

    return Insights(
        throughput_pct=throughput,
        new_trend_pct=_pct_diff(s.today_new, s.yesterday_new),
        done_trend_pct=_pct_diff(s.today_done, s.yesterday_done),
        backlog_level=backlog_level,
        backlog_severity=severity,
        top_busy=top_busy,
        concentration_ratio=concentration_ratio,
        alerts=alerts,
    )


# ============================================================================
# 6) UI Renderers
# ============================================================================

# --- 공용 유틸 ---

def truncate(text: str, max_len: int) -> str:
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 1)] + "…"


def humanize_age(hours: float) -> str:
    if hours < 1:
        return f"{max(0, int(hours * 60))}분"
    if hours < 24:
        return f"{int(hours)}시간"
    return f"{int(hours / 24)}일"


def _trend_indicator(today: int, yesterday: int, theme: Theme, *, higher_is_better: bool) -> Text:
    """전일 대비 추세 (상태에 맞는 색상 부여)."""
    if today == 0 and yesterday == 0:
        return Text("―", style=theme.muted)
    if yesterday == 0 and today > 0:
        return Text("▲ NEW", style=theme.ok if higher_is_better else theme.warn)
    diff = (today - yesterday) / yesterday * 100.0
    if diff > 0:
        col = theme.ok if higher_is_better else (theme.warn if diff >= 50 else theme.info)
        return Text(f"▲ {diff:+.1f}%", style=col)
    if diff < 0:
        col = theme.warn if higher_is_better else theme.ok
        return Text(f"▼ {diff:+.1f}%", style=col)
    return Text("― 0.0%", style=theme.muted)


# --- 헤더 (배너만, 컨테이너 없음) ---

BANNER_LINES = [
    " ██╗ ████████╗      ███████╗  ███████╗ ███████╗ ██╗   ██╗",
    " ██║ ╚══██╔══╝      ██╔═══██╗ ██╔════╝ ██╔════╝ ██║  ██╔╝",
    " ██║    ██║         ██║   ██║ ███████╗ ███████╗ ██████╔╝ ",
    " ██║    ██║         ██║   ██║ ██╔════╝ ╚════██║ ██╔══██╗",
    " ██║    ██║         ███████╔╝ ███████╗ ███████║ ██║   ██╗",
    " ╚═╝    ╚═╝         ╚══════╝  ╚══════╝ ╚══════╝ ╚═╝   ╚═╝",
]


def render_header(
    theme: Theme,
    schema_label: str | None,
    *,
    live_spinner: str = "•",
    live_dot_on: bool = False,
    live_state: str = "ok",  # ok / warn / danger
    last_ok_text: str | None = None,
) -> RenderableType:
    live_color = (
        theme.ok if live_state == "ok"
        else theme.warn if live_state == "warn"
        else theme.danger
    )
    dot_text = "***" if live_dot_on else "..."
    spinner_text = f"{live_spinner}{live_spinner}{live_spinner}"
    last_ok = last_ok_text or "--:--:--"

    left = Text(f"[LIVE {spinner_text}]", style=f"bold {live_color}")
    center = Text("", style=theme.info)
    right = Text(f"[RT {dot_text} {last_ok}]", style=f"bold {live_color}")

    indicator_row = Table.grid(expand=True)
    indicator_row.add_column(justify="left", ratio=1)
    indicator_row.add_column(justify="center", ratio=2)
    indicator_row.add_column(justify="right", ratio=1)
    indicator_row.add_row(left, center, right)
    return indicator_row


# --- KPI 카드 ---

def _kpi_card(label: str, value: str, sub: RenderableType, accent: str, theme: Theme) -> Panel:
    label_t = Text(label, style=f"bold {theme.muted}", justify="center")
    value_t = Text(value, style=f"bold {accent}", justify="center")
    sub_renderable = sub if isinstance(sub, (Text, Group)) else Text(str(sub))
    if isinstance(sub_renderable, Text):
        sub_renderable.justify = "center"
    body = Group(label_t, value_t, Align.center(sub_renderable))
    return Panel(
        Padding(body, (0, 1)),
        box=box.SQUARE,
        border_style=accent,
        padding=(0, 0),
    )


def render_kpi_strip(
    data: DashboardData,
    theme: Theme,
    *,
    new_alert_delta: int = 0,
    pulse_on: bool = False,
) -> RenderableType:
    s = data.summary
    ins = data.insights

    new_card_color = theme.warn if (new_alert_delta > 0 and pulse_on) else theme.info
    new_sub = _trend_indicator(s.today_new, s.yesterday_new, theme, higher_is_better=False)
    if new_alert_delta > 0:
        new_sub = Text(f"🔔 신규 +{new_alert_delta}", style=f"bold {theme.warn}" if pulse_on else theme.info)

    card_new = _kpi_card(
        "오늘 신규",
        f"{s.today_new}건",
        new_sub,
        new_card_color,
        theme,
    )
    card_done = _kpi_card(
        "오늘 완료",
        f"{s.today_done}건",
        _trend_indicator(s.today_done, s.yesterday_done, theme, higher_is_better=True),
        theme.ok,
        theme,
    )

    if ins.throughput_pct is None:
        rate_value, rate_color = "—", theme.muted
        rate_sub: RenderableType = Text("신규 없음", style=theme.muted)
    else:
        rate_value = f"{ins.throughput_pct:.0f}%"
        rate_color = (
            theme.ok if ins.throughput_pct >= 80
            else theme.warn if ins.throughput_pct >= 40
            else theme.danger
        )
        rate_sub = Text(f"백로그 {ins.backlog_level}", style=theme.severity_color(ins.backlog_severity))
    card_rate = _kpi_card("처리율", rate_value, rate_sub, rate_color, theme)

    pending_color = theme.severity_color(ins.backlog_severity)
    pending_sub = Text()
    pending_sub.append("미배정 ", style=theme.muted)
    pending_sub.append(
        f"{s.unassigned_pending}",
        style=f"bold {theme.warn}" if s.unassigned_pending > 0 else theme.muted,
    )
    pending_sub.append("  ·  노후 ", style=theme.muted)
    pending_sub.append(
        f"{s.stale_open}",
        style=f"bold {theme.warn}" if s.stale_open > 0 else theme.muted,
    )
    card_pending = _kpi_card("미처리", f"{s.pending_tickets}건", pending_sub, pending_color, theme)

    # Columns(expand=True)가 의도대로 안 펼쳐지는 경우가 있어 Table.grid로 폭을 4등분
    grid = Table.grid(expand=True, padding=(0, 1))
    for _ in range(4):
        grid.add_column(ratio=1)
    grid.add_row(card_new, card_done, card_rate, card_pending)
    return Padding(grid, (0, 1))  # 좌우 여백 최소화


# --- 알림 영역 ---

def render_alerts(
    insights: Insights,
    theme: Theme,
    *,
    new_event_message: str | None = None,
) -> RenderableType:
    if not insights.alerts and not new_event_message:
        body = Text("✓  특이 신호 없음 — 모든 운영 지표가 정상 범위입니다.", style=theme.ok)
        return Panel(
            body, box=box.SQUARE, border_style=theme.ok,
            title=f"[bold {theme.ok}]상황 신호[/]", title_align="left",
        )

    icon_map = {"info": "•", "warn": "▲", "danger": "■"}
    severity_order = {"danger": 0, "warn": 1, "info": 2}
    sorted_alerts = sorted(insights.alerts, key=lambda a: severity_order.get(a.level, 3))

    lines: list[Text] = []
    if new_event_message:
        msg = Text()
        msg.append(" ✦ ", style=f"bold {theme.warn}")
        msg.append(new_event_message, style=f"bold {theme.warn}")
        lines.append(msg)

    for a in sorted_alerts:
        col = theme.severity_color(a.level)
        line = Text()
        line.append(f" {icon_map.get(a.level, '•')} ", style=f"bold {col}")
        line.append(a.title, style=f"bold {col}")
        line.append("   ")
        line.append(a.detail, style=theme.muted)
        lines.append(line)

    overall = theme.danger if any(a.level == "danger" for a in insights.alerts) else theme.warn
    return Panel(
        Group(*lines),
        box=box.SQUARE, border_style=overall,
        title=f"[bold {overall}]상황 신호 ({len(insights.alerts)})[/]",
        title_align="left",
    )


# --- 최근 요청 ---

def render_recent_requests(rows: list[RecentRequest], theme: Theme, compact: bool, view: ViewOptions) -> Panel:
    table = Table(
        box=box.SIMPLE_HEAVY, expand=True,
        show_edge=False, pad_edge=False, show_lines=False,
        header_style=f"bold {theme.accent}",
    )
    table.add_column("ID", justify="right", width=6, style=theme.muted)
    table.add_column("요청제목", overflow="fold", ratio=4)
    if view.recent_show_category:
        table.add_column("카테고리", overflow="ellipsis", no_wrap=True, min_width=12, ratio=2, style=theme.muted)
    if view.recent_show_requester:
        table.add_column("요청자", overflow="fold", ratio=1)
    table.add_column("담당자", overflow="fold", ratio=2)
    table.add_column("상태", justify="center", width=8)
    if view.recent_show_created:
        table.add_column("등록", justify="center", width=11, style=theme.muted)
    table.add_column("경과", justify="right", width=6)

    if not rows:
        empty = ["-"]
        empty.append("데이터 없음")
        if view.recent_show_category:
            empty.append("-")
        if view.recent_show_requester:
            empty.append("-")
        # 담당자, 상태
        empty.extend(["-", "-"])
        # 등록(옵션)
        if view.recent_show_created:
            empty.append("-")
        # 경과
        empty.append("-")
        table.add_row(*empty)
    else:
        for r in rows:
            is_completed = r.status in ("resolved", "closed")
            status_color = theme.status_color(r.status)

            title_text = Text(truncate(r.title, RECENT_TITLE_MAX))
            if r.is_unassigned and r.status in ("open", "in_progress"):
                title_text.stylize(f"bold {theme.warn}")
            elif r.priority == "urgent" and not is_completed:
                title_text.stylize("bold")

            assignee_text = (
                Text("미배정", style=f"bold {theme.warn}")
                if r.is_unassigned else Text(r.assignee or "-")
            )

            # 경과 시간 색상 (미해결 상태일 때만 강조)
            age_color = theme.muted
            if r.status in ("open", "in_progress"):
                if r.age_hours >= STALE_THRESHOLD_DAYS * 24:
                    age_color = theme.danger
                elif r.age_hours >= 24:
                    age_color = theme.warn

            cells = [Text(f"#{r.id}", style=theme.muted), title_text]
            if view.recent_show_category:
                cells.append(Text(truncate(r.category, RECENT_CATEGORY_MAX), style=theme.muted))
            if view.recent_show_requester:
                cells.append(Text(r.requester))
            cells.extend([
                assignee_text,
                Text(STATUS_LABEL.get(r.status, r.status), style=status_color),
            ])
            if view.recent_show_created:
                cells.append(Text(r.created_at_kst, style=theme.muted))
            cells.append(Text(humanize_age(r.age_hours), style=age_color))

            # 완료/사업검토 건은 dim 처리 → 미처리 행이 시각적으로 도드라짐
            if is_completed:
                for cell in cells:
                    cell.stylize("dim")

            table.add_row(*cells)

    pending_count = sum(1 for r in rows if r.status in ("open", "in_progress"))
    done_count = len(rows) - pending_count
    title = Text()
    title.append("요청 현황", style=f"bold {theme.accent}")
    title.append("  ", style=theme.muted)
    title.append(f"미처리 {pending_count}",
                 style=f"bold {theme.warn}" if pending_count > 0 else theme.muted)
    title.append(" · ", style=theme.muted)
    title.append(f"완료 {done_count}", style=theme.muted)
    return Panel(table, box=box.SQUARE, border_style=theme.muted, title=title, title_align="left")


# --- 담당자 워크로드 ---

def render_assignee_workload(rows: list[AssigneeWorkload], theme: Theme, view: ViewOptions) -> Panel:
    table = Table(
        box=box.SIMPLE, expand=True, show_edge=False, pad_edge=False,
        header_style=f"bold {theme.accent}",
    )
    table.add_column("순위", justify="right", width=4, style=theme.muted)
    table.add_column("담당자", overflow="ellipsis", no_wrap=True, ratio=1)
    table.add_column("미처리", justify="right", width=6)
    table.add_column("오늘", justify="right", width=6)
    if view.workload_show_week:
        table.add_column("이번주", justify="right", width=7)
    if view.workload_show_total:
        table.add_column("총", justify="right", width=6)

    if not rows:
        empty = ["-", "데이터 없음", "0", "0"]
        if view.workload_show_week:
            empty.append("0")
        if view.workload_show_total:
            empty.append("0")
        table.add_row(*empty)
        return Panel(
            table, box=box.SQUARE, border_style=theme.muted,
            title=f"[bold {theme.accent}]담당자 워크로드[/]", title_align="left",
        )
    max_pending = max((w.pending_count for w in rows), default=1)
    rank = 0
    for w in rows:
        if w.is_unassigned:
            rank_label = Text("─", style=theme.muted)
            assignee_text = Text(
                w.assignee,
                style=f"bold {theme.warn}" if w.pending_count > 0 else theme.muted,
            )
            pending_color = theme.warn if w.pending_count > 0 else theme.muted
        else:
            rank += 1
            rank_label = Text(str(rank), style=theme.muted)
            assignee_text = Text(w.assignee)
            if w.pending_count >= max(5, max_pending * 0.7):
                pending_color = theme.warn
            elif w.pending_count > 0:
                pending_color = theme.info
            else:
                pending_color = theme.muted

        done_color = theme.ok if w.done_today_count > 0 else theme.muted
        cells = [
            rank_label,
            assignee_text,
            Text(str(w.pending_count), style=f"bold {pending_color}"),
            Text(str(w.done_today_count), style=done_color),
        ]
        if view.workload_show_week:
            cells.append(Text(str(w.done_week_count), style=theme.info if w.done_week_count > 0 else theme.muted))
        if view.workload_show_total:
            cells.append(Text(str(w.done_total_count), style=theme.info if w.done_total_count > 0 else theme.muted))
        table.add_row(*cells)

    return Panel(
        table, box=box.SQUARE, border_style=theme.muted,
        title=f"[bold {theme.accent}]담당자 워크로드[/]", title_align="left",
    )


# --- 오늘 카테고리 ---

def render_category_today(rows: list[CategoryToday], theme: Theme, view: ViewOptions) -> Panel:
    table = Table(
        box=box.SIMPLE, expand=True, show_edge=False, pad_edge=False,
        header_style=f"bold {theme.accent}",
    )
    table.add_column("카테고리", no_wrap=True, overflow="ellipsis", ratio=1)
    table.add_column("오늘", justify="right", width=6)
    table.add_column("총", justify="right", width=6)
    if view.bottom_show_week:
        table.add_column("이번주", justify="right", width=7)

    if not rows:
        empty = ["데이터 없음", "0", "0"]
        if view.bottom_show_week:
            empty.append("0")
        table.add_row(*empty)
    else:
        for r in rows:
            color = theme.info if (r.today_count > 0 or r.week_count > 0 or r.total_count > 0) else theme.muted
            cells: list[RenderableType] = [
                Text(r.category, style=color),
                Text(str(r.today_count), style=f"bold {color}" if r.today_count > 0 else theme.muted),
                Text(str(r.total_count), style=theme.info if r.total_count > 0 else theme.muted),
            ]
            if view.bottom_show_week:
                cells.append(Text(str(r.week_count), style=theme.info if r.week_count > 0 else theme.muted))
            table.add_row(*cells)

    return Panel(
        table, box=box.SQUARE, border_style=theme.muted,
        title=f"[bold {theme.accent}]카테고리 현황[/]", title_align="left",
    )


def render_work_type_today(rows: list[WorkTypeToday], theme: Theme, view: ViewOptions) -> Panel:
    table = Table(
        box=box.SIMPLE, expand=True, show_edge=False, pad_edge=False,
        header_style=f"bold {theme.accent}",
    )
    table.add_column("작업 유형", no_wrap=True, overflow="ellipsis", ratio=1)
    table.add_column("오늘", justify="right", width=6)
    table.add_column("총", justify="right", width=6)
    if view.bottom_show_week:
        table.add_column("이번주", justify="right", width=7)

    if not rows:
        empty = ["데이터 없음", "0", "0"]
        if view.bottom_show_week:
            empty.append("0")
        table.add_row(*empty)
    else:
        for r in rows:
            color = theme.info if (r.today_count > 0 or r.week_count > 0 or r.total_count > 0) else theme.muted
            cells: list[RenderableType] = [
                Text(r.work_type, style=color),
                Text(str(r.today_count), style=f"bold {color}" if r.today_count > 0 else theme.muted),
                Text(str(r.total_count), style=theme.info if r.total_count > 0 else theme.muted),
            ]
            if view.bottom_show_week:
                cells.append(Text(str(r.week_count), style=theme.info if r.week_count > 0 else theme.muted))
            table.add_row(*cells)

    return Panel(
        table, box=box.SQUARE, border_style=theme.muted,
        title=f"[bold {theme.accent}]작업 유형 현황[/]", title_align="left",
    )


# --- 7일 추세 ---

def render_trend(rows: list[TrendPoint], theme: Theme) -> Panel:
    counts = [r.count for r in rows]
    avg = sum(counts) / len(counts) if counts else 0
    today = counts[-1] if counts else 0
    delta = today - avg

    today_color = theme.warn if delta > 0 else (theme.ok if delta < 0 else theme.muted)
    summary = Text()
    summary.append("평균 ", style=theme.muted)
    summary.append(f"{avg:.1f}건", style=theme.info)
    summary.append("   오늘 ", style=theme.muted)
    summary.append(f"{today}건 ({delta:+.1f})", style=today_color)

    # 날짜/건수를 가로 2행으로 표시 → 세로 공간 대폭 절약
    grid = Table(
        box=None, expand=True, show_edge=False, pad_edge=False, show_header=False,
        padding=(0, 0),
    )
    for _ in rows or [None]:
        grid.add_column(justify="center")
    if rows:
        max_count = max(counts) if counts else 0
        grid.add_row(*[Text(r.label, style=theme.muted) for r in rows])
        count_cells: list[Text] = []
        for r in rows:
            if r.count == 0:
                style = theme.muted
            elif max_count and r.count == max_count:
                style = f"bold {theme.accent}"
            else:
                style = theme.info
            count_cells.append(Text(str(r.count), style=style))
        grid.add_row(*count_cells)
    else:
        grid.add_row(Text("데이터 없음", style=theme.muted))

    body = Group(
        Align.center(summary),
        Rule(style=theme.muted),
        grid,
    )
    return Panel(
        body, box=box.SQUARE, border_style=theme.muted,
        title=f"[bold {theme.accent}]최근 5일 신규 추세[/]", title_align="left",
    )


# --- 푸터 ---

def render_footer(
    theme: Theme,
    refreshed: datetime,
    refresh_sec: int,
    schema_label: str | None,
    paused: bool,
    mode: str,
    last_error: str | None,
) -> Panel:
    refreshed_str = refreshed.strftime("%Y-%m-%d %H:%M:%S")
    parts: list[Text] = []

    t = Text()
    t.append("갱신 ", style=theme.muted); t.append(refreshed_str, style=theme.info)
    parts.append(t)

    t = Text()
    t.append("주기 ", style=theme.muted)
    t.append("일시정지" if paused else f"{refresh_sec}초",
             style=theme.warn if paused else theme.info)
    parts.append(t)

    t = Text()
    t.append("모드 ", style=theme.muted); t.append(mode, style=theme.info)
    parts.append(t)

    t = Text()
    t.append("schema ", style=theme.muted); t.append(schema_label or "default", style=theme.info)
    parts.append(t)

    if last_error:
        t = Text()
        t.append("⚠ 최근 갱신 실패 ", style=theme.danger); t.append("(이전 데이터 표시중)", style=theme.muted)
        parts.append(t)

    keys = Text()
    for k, label in [("q", "종료"), ("r", "새로고침"), ("p", "일시정지"),
                     ("c", "컴팩트"), ("t", "테마"), ("+/-", "주기")]:
        keys.append(f" {k} ", style=f"bold black on {theme.accent}")
        keys.append(f" {label}  ", style=theme.muted)
    parts.append(keys)

    return Panel(
        Columns(parts, expand=True, equal=False, padding=(0, 2)),
        box=box.SQUARE, border_style=theme.muted,
    )


# --- 전체 레이아웃 ---

def build_layout(
    data: DashboardData,
    theme: Theme,
    schema_label: str | None,
    view: ViewOptions,
    refresh_sec: int,
    compact: bool,
    paused: bool,
    last_error: str | None,
    new_alert_delta: int = 0,
    pulse_on: bool = False,
    new_alert_time: str | None = None,
    live_spinner: str = "•",
    live_dot_on: bool = False,
    live_state: str = "ok",
    last_ok_text: str | None = None,
) -> Layout:
    root = Layout(name="root")
    header_size = 1
    kpi_size = 5
    alert_size = max(3, min(8, 2 + max(0, len(data.insights.alerts))))
    footer_size = 0
    outer_margin = 2 if compact else 3
    gap = 1

    # 화면 바깥 좌우 여백
    root.split_row(
        Layout(name="outer_left", size=outer_margin),
        Layout(name="content", ratio=1),
        Layout(name="outer_right", size=outer_margin),
    )
    root["outer_left"].update(Text(""))
    root["outer_right"].update(Text(""))

    # 상하 섹션 간 간격
    root["content"].split_column(
        Layout(name="header", size=header_size),
        Layout(name="v_gap_1", size=gap),
        Layout(name="kpi", size=kpi_size),
        Layout(name="v_gap_2", size=gap),
        Layout(name="alert", size=alert_size),
        Layout(name="v_gap_3", size=gap),
        Layout(name="body", ratio=1),
    )
    root["v_gap_1"].update(Text(""))
    root["v_gap_2"].update(Text(""))
    root["v_gap_3"].update(Text(""))

    # 본문: 좌측(요청현황 + 작업유형/카테고리), 우측(5일 추세/워크로드) 구조
    workload_rows = len(data.by_assignee_workload)
    category_rows = len(data.by_category_today)
    work_type_rows = len(data.by_work_type_today)
    # 요청현황은 행 수만큼만 채워 공간을 줄이고, 작업유형·카테고리 전체 행 표시에 넘김
    recent_size = max(8, len(data.recent_requests) + RECENT_PANEL_OVERHEAD)
    bottom_panel_chrome = 4
    category_size = max(6, category_rows + bottom_panel_chrome)
    work_type_size = max(6, work_type_rows + bottom_panel_chrome)
    left_bottom_size = max(category_size, work_type_size)
    workload_size = max(6, workload_rows + WORKLOAD_PANEL_OVERHEAD)

    # 본문 좌/우 폭. 우측 컬럼(추세/워크로드)은 동일 너비로 유지
    root["body"].split_row(
        Layout(name="left_main", ratio=5),
        Layout(name="h_gap", size=gap),
        Layout(name="right_main", ratio=2),
    )
    root["h_gap"].update(Text(""))

    # 좌측: 최근 요청 + 하단(작업유형/카테고리)
    left_gap_size = 0 if compact else 0
    root["left_main"].split_column(
        Layout(name="recent", size=recent_size),
        Layout(name="left_gap", size=left_gap_size),
        Layout(name="left_bottom", size=left_bottom_size),
        Layout(name="left_spacer", ratio=1),
    )
    root["left_gap"].update(Text(""))
    root["left_spacer"].update(Text(""))
    bottom_gap = 1 if compact else 2
    root["left_bottom"].split_row(
        Layout(name="work_type", ratio=1),
        Layout(name="left_bottom_gap", size=bottom_gap),
        Layout(name="category", ratio=1),
    )
    root["left_bottom_gap"].update(Text(""))

    # 우측: 최근 5일 신규 추세 / 담당자 워크로드(행 수에 맞춘 고정 높이, 나머지는 spacer)
    root["right_main"].split_column(
        Layout(name="trend", size=8),
        Layout(name="right_gap_1", size=gap),
        Layout(name="workload", size=workload_size),
        Layout(name="right_spacer", ratio=1),
    )
    root["right_gap_1"].update(Text(""))
    root["right_spacer"].update(Text(""))

    root["header"].update(render_header(
        theme,
        schema_label,
        live_spinner=live_spinner,
        live_dot_on=live_dot_on,
        live_state=live_state,
        last_ok_text=last_ok_text,
    ))
    root["kpi"].update(render_kpi_strip(
        data, theme,
        new_alert_delta=new_alert_delta,
        pulse_on=pulse_on,
    ))
    new_event_message = None
    if new_alert_delta > 0:
        when = f" ({new_alert_time})" if new_alert_time else ""
        new_event_message = f"신규 요청 +{new_alert_delta}건 유입{when}"
    root["alert"].update(render_alerts(data.insights, theme, new_event_message=new_event_message))
    root["recent"].update(render_recent_requests(data.recent_requests, theme, compact, view))
    root["workload"].update(Padding(render_assignee_workload(data.by_assignee_workload, theme, view), (0, 1)))
    root["trend"].update(Padding(render_trend(data.trend_7d, theme), (0, 1)))
    root["category"].update(render_category_today(data.by_category_today, theme, view))
    root["work_type"].update(render_work_type_today(data.by_work_type_today, theme, view))
    return root


def render_error_view(message: str, theme: Theme, retry_in_sec: int) -> Panel:
    body = Group(
        Text("⚠ DB 연결/조회 오류", style=f"bold {theme.danger}"),
        Text(""),
        Text(message, style=theme.warn, overflow="fold"),
        Text(""),
        Text(f"{retry_in_sec}초 후 자동 재시도합니다. 즉시 재시도는 'r' 키.", style=theme.muted),
        Text("DB 연결 정보(--db-url) 또는 네트워크/방화벽을 확인하세요.", style=theme.muted),
    )
    return Panel(
        body, box=box.HEAVY, border_style=theme.danger,
        title=f"[bold {theme.danger}] 시스템 알림 [/]",
    )


# ============================================================================
# 7) Key Listener (선택적, 실패 시 조용히 비활성화)
# ============================================================================

class KeyListener:
    """OS별 비차단 키 리더. 실패하면 자동 갱신만 동작."""

    def __init__(self) -> None:
        self._key: str | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._original_termios = None
        self.enabled = False

    def start(self) -> None:
        try:
            if os.name == "nt":
                self._thread = threading.Thread(target=self._win_loop, daemon=True)
                self._thread.start()
                self.enabled = True
            else:
                import termios
                import tty
                fd = sys.stdin.fileno()
                self._original_termios = termios.tcgetattr(fd)
                tty.setcbreak(fd)
                self._thread = threading.Thread(target=self._unix_loop, daemon=True)
                self._thread.start()
                self.enabled = True
        except Exception:
            self.enabled = False

    def stop(self) -> None:
        self._stop.set()
        if self._original_termios is not None:
            try:
                import termios
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self._original_termios)
            except Exception:
                pass

    def _win_loop(self) -> None:
        import msvcrt  # type: ignore
        while not self._stop.is_set():
            if msvcrt.kbhit():
                try:
                    ch = msvcrt.getwch()
                except Exception:
                    ch = ""
                if ch:
                    with self._lock:
                        self._key = ch
            time.sleep(0.05)

    def _unix_loop(self) -> None:
        import select
        while not self._stop.is_set():
            r, _, _ = select.select([sys.stdin], [], [], 0.05)
            if r:
                try:
                    ch = sys.stdin.read(1)
                except Exception:
                    ch = ""
                if ch:
                    with self._lock:
                        self._key = ch

    def consume(self) -> str | None:
        with self._lock:
            k = self._key
            self._key = None
        return k


# ============================================================================
# 8) App Controller
# ============================================================================

@dataclass
class AppState:
    refresh_sec: int
    compact: bool
    theme_name: str
    paused: bool = False

    def theme(self) -> Theme:
        return THEMES.get(self.theme_name, THEMES["dark"])

    def cycle_theme(self) -> None:
        names = list(THEMES.keys())
        idx = names.index(self.theme_name) if self.theme_name in names else 0
        self.theme_name = names[(idx + 1) % len(names)]


def _connect_with_backoff(db_url: str, attempt: int) -> psycopg.Connection:
    """지수 백오프 재접속. 호출자에서 try/except로 감쌀 것."""
    if attempt > 1:
        delay = min(30.0, 1.0 * (2 ** (attempt - 1)))
        time.sleep(delay)
    return psycopg.connect(db_url, connect_timeout=10)


def run_loop(args: argparse.Namespace) -> int:
    db_url = normalize_database_url(args.db_url)
    schema_label = parse_search_path_from_url(args.db_url)

    state = AppState(
        refresh_sec=args.refresh_sec,
        compact=args.compact,
        theme_name=args.theme,
    )
    console = Console()
    listener = KeyListener()
    if not args.once:
        listener.start()

    stop = threading.Event()

    def _handle_signal(_signum: int, _frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    try:
        signal.signal(signal.SIGTERM, _handle_signal)
    except (AttributeError, ValueError):
        # Windows 등에서 SIGTERM 미지원 시 무시
        pass

    last_data: DashboardData | None = None
    last_error: str | None = None
    next_refresh_at: float = 0.0
    conn: psycopg.Connection | None = None
    reconnect_attempt = 0
    new_alert_until: float = 0.0
    new_alert_delta: int = 0
    new_alert_time: str | None = None
    last_success_mono: float | None = None

    def _ensure_connection() -> psycopg.Connection:
        nonlocal conn, reconnect_attempt
        if conn is not None and not conn.closed:
            return conn
        reconnect_attempt += 1
        conn = _connect_with_backoff(db_url, reconnect_attempt)
        reconnect_attempt = 0
        return conn

    def _do_fetch(current_mono: float | None = None) -> None:
        nonlocal last_data, last_error, conn, new_alert_until, new_alert_delta, new_alert_time, last_success_mono
        try:
            c = _ensure_connection()
            repo = DashboardRepository(c)
            data = repo.fetch(args.limit_recent)
            data.insights = compute_insights(data)

            # 오늘 신규 건수 증가 감지 -> 짧은 펄스/토스트 표시
            if last_data is not None:
                prev_today_new = int(last_data.summary.today_new or 0)
                curr_today_new = int(data.summary.today_new or 0)
                delta = curr_today_new - prev_today_new
                if delta > 0:
                    now_mono = current_mono if current_mono is not None else time.monotonic()
                    new_alert_until = now_mono + NEW_ALERT_DURATION_SEC
                    new_alert_delta = delta
                    new_alert_time = datetime.now().strftime("%H:%M:%S")

            last_data = data
            last_success_mono = current_mono if current_mono is not None else time.monotonic()
            last_error = None
        except Exception as exc:
            last_error = str(exc)
            LOGGER.exception("데이터 조회 실패")
            try:
                if conn is not None and not conn.closed:
                    conn.close()
            except Exception:
                pass
            conn = None

    if args.once:
        _do_fetch(time.monotonic())
        if last_data is not None:
            once_size = console.size
            once_view = derive_view_options(once_size.width, once_size.height, state.compact)
            console.print(build_layout(
                last_data, state.theme(), schema_label,
                once_view,
                state.refresh_sec, state.compact, state.paused, last_error,
                new_alert_delta=0,
                pulse_on=False,
                new_alert_time=None,
                live_spinner=LIVE_SPINNER_FRAMES[0],
                live_dot_on=True,
                live_state="ok",
                last_ok_text=last_data.refreshed_at.strftime("%H:%M:%S"),
            ))
            return 0
        console.print(render_error_view(last_error or "알 수 없는 오류", state.theme(), 0))
        return 1

    use_screen = not args.no_screen
    try:
        with Live(console=console, refresh_per_second=8, screen=use_screen) as live:
            while not stop.is_set():
                now = time.monotonic()

                # 키 입력 처리
                key = listener.consume()
                if key:
                    if key in ("q", "Q", "\x1b"):
                        stop.set()
                        break
                    elif key in ("r", "R"):
                        next_refresh_at = 0.0
                    elif key in ("p", "P"):
                        state.paused = not state.paused
                    elif key in ("c", "C"):
                        state.compact = not state.compact
                    elif key in ("t", "T"):
                        state.cycle_theme()
                    elif key in ("+", "="):
                        state.refresh_sec = min(600, state.refresh_sec + 5)
                    elif key in ("-", "_"):
                        state.refresh_sec = max(2, state.refresh_sec - 5)

                # 데이터 갱신
                if not state.paused and now >= next_refresh_at:
                    _do_fetch(now)
                    next_refresh_at = now + state.refresh_sec

                # 화면 업데이트
                if last_data is not None:
                    term_size = console.size
                    view = derive_view_options(term_size.width, term_size.height, state.compact)
                    spinner_idx = int(now / LIVE_SPINNER_STEP_SEC) % len(LIVE_SPINNER_FRAMES)
                    live_spinner = LIVE_SPINNER_FRAMES[spinner_idx]
                    live_dot_on = (int(now / LIVE_BLINK_SEC) % 2 == 0)
                    age_since_ok = (now - last_success_mono) if last_success_mono is not None else 999999
                    if last_error:
                        live_state = "danger"
                    elif age_since_ok > (state.refresh_sec * 2):
                        live_state = "warn"
                    else:
                        live_state = "ok"

                    pulse_active = now < new_alert_until
                    pulse_on = pulse_active and (int(now / NEW_ALERT_BLINK_SEC) % 2 == 0)
                    visible_new_delta = new_alert_delta if pulse_active else 0
                    live.update(build_layout(
                        last_data, state.theme(), schema_label,
                        view,
                        state.refresh_sec, state.compact, state.paused, last_error,
                        new_alert_delta=visible_new_delta,
                        pulse_on=pulse_on,
                        new_alert_time=new_alert_time if pulse_active else None,
                        live_spinner=live_spinner,
                        live_dot_on=live_dot_on,
                        live_state=live_state,
                        last_ok_text=last_data.refreshed_at.strftime("%H:%M:%S"),
                    ))
                else:
                    retry_in = max(0, int(next_refresh_at - now))
                    live.update(render_error_view(
                        last_error or "초기화 중", state.theme(), retry_in,
                    ))

                time.sleep(0.1)
    finally:
        listener.stop()
        try:
            if conn is not None and not conn.closed:
                conn.close()
        except Exception:
            pass

    return 0


# ============================================================================
# 9) CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="IT Desk 터미널 운영 대시보드 (Rich 기반)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", DEFAULT_DB_URL),
        help="PostgreSQL 연결 URL (DATABASE_URL 환경변수 우선)",
    )
    parser.add_argument(
        "--refresh-sec", type=int,
        default=int(os.getenv("DASHBOARD_REFRESH_SEC", str(DEFAULT_REFRESH_SEC))),
        help="자동 갱신 주기(초). 권장 범위 5~120",
    )
    parser.add_argument(
        "--limit-recent", type=int, default=DEFAULT_RECENT_LIMIT,
        help="최근 요청 표시 개수 (1~100)",
    )
    parser.add_argument(
        "--theme", choices=sorted(THEMES.keys()), default="dark",
        help="컬러 테마",
    )
    parser.add_argument(
        "--compact", action="store_true",
        help="컴팩트 레이아웃 (좁은 터미널용 — 카테고리 컬럼 등 축소)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="한 번만 조회하고 종료 (스냅샷 출력)",
    )
    parser.add_argument(
        "--no-screen", action="store_true",
        help="대체 화면 모드 비활성화 (디버그/스크롤백 유지)",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="디버그 로그 출력 (stderr)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.refresh_sec < 1:
        print("ERROR: --refresh-sec는 1 이상이어야 합니다.", file=sys.stderr)
        return 2
    if args.limit_recent < 1 or args.limit_recent > 100:
        print("ERROR: --limit-recent는 1~100 사이여야 합니다.", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        return run_loop(args)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        LOGGER.exception("대시보드 실행 실패")
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
