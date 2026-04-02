"""
통신 운영 의사결정 에이전트 — Snowflake Streamlit (Streamlit in Snowflake)
하이브리드 방식: Cortex Analyst SQL + Cortex Complete + Snowpark get_active_session()
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

import streamlit as st
from snowflake.snowpark.context import get_active_session

# -----------------------------------------------------------------------------
# 설정
# -----------------------------------------------------------------------------
DEFAULT_ACCOUNT = "SQHVTHB-UX70775"
DEFAULT_WAREHOUSE = "COMPUTE_WH"
DEFAULT_DATABASE = "HACKATHON_DB"
DEFAULT_SCHEMA = "ANALYTICS"
CORTEX_MODEL = "mistral-large2"

STATIC_CONTEXT = """
[분석 가이드라인]
- 마케팅: 채널별 CVR 격차가 클 수 있음. SQL 결과로 재확인할 것.
- 퍼널: 상담 유입 단계에서 병목 가능성 있음. 단계별 CVR 확인 필요.
- CS: 수신 연결률이 목표 70% 미달일 수 있음. 피크타임 확인 필요.
※ 위 내용은 가설임. 반드시 SQL 결과 숫자를 우선 사용할 것.
  SQL 결과가 없으면 해당 필드는 null로 두고 추측 금지.
"""

# -----------------------------------------------------------------------------
# 하이브리드: Cortex Analyst Playground에서 생성한 SQL 하드코딩
# -----------------------------------------------------------------------------
PREBUILT_SQLS = {
    "marketing": """
SELECT
  utm_source,
  SUM(ga_total_contracts) / NULLIF(SUM(ga_total_sessions), 0) AS contract_cvr,
  SUM(ga_total_contracts) AS total_contracts,
  SUM(ga_total_sessions) AS total_sessions,
  SUM(ga_total_revenue) AS total_revenue
FROM v07_ga4_marketing_attribution
GROUP BY utm_source
HAVING SUM(ga_total_sessions) > 0
ORDER BY contract_cvr DESC NULLS LAST
""",
    "funnel": """
WITH latest AS (
  SELECT *
  FROM v03_contract_funnel_conversion
  WHERE main_category_name = '렌탈'
  QUALIFY ROW_NUMBER() OVER (ORDER BY year_month DESC) = 1
),
drops AS (
  SELECT '상담요청 to 접수' AS transition, (cvr_consult_request - cvr_registend) AS dropoff FROM latest
  UNION ALL SELECT '접수 to 개통', (cvr_registend - cvr_open) FROM latest
  UNION ALL SELECT '개통 to 지급', (cvr_open - cvr_payend) FROM latest
)
SELECT transition, dropoff
FROM drops
ORDER BY dropoff DESC NULLS LAST
LIMIT 1
""",
    "cs": """
SELECT
  SUM(hourly_connected_count) / NULLIF(SUM(hourly_call_count), 0) AS weighted_connection_rate,
  AVG(hourly_connection_rate) AS avg_connection_rate,
  SUM(hourly_call_count) AS total_calls
FROM v10_hourly_call_distribution
WHERE LOWER(COALESCE(day_of_week_name, '')) IN ('일', '일요일', 'sunday', 'sun')
""",
}

DEMO_QUESTION_DOMAIN_MAP = {
    "어떤 마케팅 채널이 제일 효율적이야?": "marketing",
    "렌탈 퍼널에서 이탈이 가장 많은 단계는?": "funnel",
    "일요일 콜센터 연결률은?": "cs",
}

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


# -----------------------------------------------------------------------------
# Step 1 — Intent Parsing
# -----------------------------------------------------------------------------
def parse_intent(user_question: str) -> dict[str, Any]:
    q = user_question.lower()
    domains: set[str] = set()

    if any(k in user_question or k in q for k in (
        "마케팅", "채널", "cvr", "utm", "광고", "예산",
        "키워드", "네이버", "카카오", "구글", "ga", "유입",
    )):
        domains.add("marketing")

    if any(k in user_question or k in q for k in (
        "영업", "계약", "강남", "구", "지역", "설치", "순매출", "매출",
    )):
        domains.add("sales")

    if any(k in user_question or k in q for k in (
        "cs", "콜", "콜센터", "연결", "인력", "상담원", "통화", "수신", "발신",
    )):
        domains.add("cs")

    if not domains:
        domains.add("integrated")

    action_hints: list[str] = []
    if "예산" in user_question or "줄이" in user_question:
        action_hints.append("cut_budget_channels")
    if "강남" in user_question or "지역" in user_question:
        action_hints.append("regional_focus")
    if "인력" in user_question or "부족" in user_question:
        action_hints.append("cs_staffing")

    return {
        "domains": sorted(domains),
        "action_hints": action_hints,
        "raw_question": user_question,
    }


# -----------------------------------------------------------------------------
# Step 2 — SQL 실행
# -----------------------------------------------------------------------------
def run_sql(session, sql: str) -> list[dict[str, Any]]:
    if not sql or not sql.strip():
        return []
    try:
        rows = session.sql(sql).collect()
    except Exception as e:
        raise RuntimeError(f"SQL 실행 실패: {e}") from e
    out: list[dict[str, Any]] = []
    for r in rows:
        if hasattr(r, "as_dict"):
            out.append(r.as_dict())
        elif hasattr(r, "_mapping"):
            out.append(dict(r._mapping))
        else:
            out.append(dict(r))
    return out


# -----------------------------------------------------------------------------
# Step 3 — Rule-based 판단
# -----------------------------------------------------------------------------
def apply_rules(rows: list[dict[str, Any]], intent: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "has_data": bool(rows),
        "marketing": {},
        "funnel": {},
        "cs": {},
        "notes": [],
    }

    if not rows:
        result["notes"].append("SQL 결과가 없거나 실행 행이 0건입니다.")
        return result

    # ── 마케팅: CVR + Revenue 복합 score + 비교 기준 ──
    best_ch = None
    best_cvr = -1.0
    best_src = None
    best_med = None
    best_score = -1.0
    best_revenue = 0.0
    best_sessions_val = 0.0
    all_cvrs: list[float] = []

    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}

        cvr = None
        for key in ("CONTRACT_CVR", "WEIGHTED_CONTRACT_CVR", "contract_cvr"):
            if key in u and u[key] is not None:
                try:
                    cvr = float(u[key])
                    break
                except (TypeError, ValueError):
                    pass

        sessions = 0.0
        try:
            sessions = float(u.get("TOTAL_SESSIONS") or u.get("total_sessions") or 0)
        except (TypeError, ValueError):
            pass

        contracts = 0.0
        try:
            contracts = float(u.get("TOTAL_CONTRACTS") or u.get("total_contracts") or 0)
        except (TypeError, ValueError):
            pass

        revenue = 0.0
        try:
            revenue = float(u.get("TOTAL_REVENUE") or u.get("total_revenue") or 0)
        except (TypeError, ValueError):
            pass

        src = u.get("UTM_SOURCE") or u.get("utm_source")
        med = u.get("UTM_MEDIUM") or u.get("utm_medium")
        channel_key = f"{src}/{med}" if (src and med) else (src or "")

        if not (cvr is not None and channel_key and sessions >= 500 and contracts >= 50):
            continue

        all_cvrs.append(cvr)
        rev_per_session = revenue / sessions if sessions > 0 else 0
        score = cvr * 0.4 + (rev_per_session / 100_000) * 0.6

        if score > best_score:
            best_score = score
            best_cvr = cvr
            best_ch = channel_key
            best_src = src
            best_med = med
            best_revenue = revenue
            best_sessions_val = sessions

    if best_ch is not None:
        avg_cvr = sum(all_cvrs) / len(all_cvrs) if all_cvrs else 0
        cvr_vs_avg = round((best_cvr - avg_cvr) / avg_cvr * 100, 1) if avg_cvr > 0 else 0
        sorted_cvrs = sorted(all_cvrs, reverse=True)
        rank = sorted_cvrs.index(best_cvr) + 1 if best_cvr in sorted_cvrs else 0

        result["marketing"] = {
            "best_channel": best_ch,
            "utm_source": best_src,
            "utm_medium": best_med,
            "cvr": best_cvr,
            "total_revenue": best_revenue,
            "total_sessions": best_sessions_val,
            "revenue_per_session": best_revenue / best_sessions_val if best_sessions_val > 0 else 0,
            "avg_cvr_of_valid_channels": round(avg_cvr, 4),
            "cvr_vs_avg_pct": cvr_vs_avg,
            "rank": rank,
            "total_channels": len(all_cvrs),
            "caution": "CVR과 매출 기여도를 함께 고려한 추천입니다.",
            "source": "sql_score_cvr_revenue",
        }
    elif "marketing" in intent.get("domains", []):
        result["notes"].append("마케팅 CVR 비교에 쓸 수 있는 숫자 열을 결과에서 찾지 못했습니다.")

    # ── 퍼널 ──
    funnel_keys = ("CVR_CONSULT_REQUEST", "CVR_REGISTEND", "CVR_OPEN", "CVR_PAYEND")
    agg: dict[str, list[float]] = {k: [] for k in funnel_keys}

    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        for k in funnel_keys:
            if k in u and u[k] is not None:
                try:
                    agg[k].append(float(u[k]))
                except (TypeError, ValueError):
                    pass

    means = {k: (sum(v) / len(v) if v else None) for k, v in agg.items()}
    valid = {k: v for k, v in means.items() if v is not None}

    if valid:
        stage = min(valid, key=valid.get)
        label_map = {
            "CVR_CONSULT_REQUEST": "상담요청",
            "CVR_REGISTEND": "접수",
            "CVR_OPEN": "개통",
            "CVR_PAYEND": "지급",
        }
        result["funnel"] = {
            "stage": label_map.get(stage, stage),
            "cvr": valid[stage],
            "source": "min_mean_stage_cvr",
        }
    else:
        for r in rows:
            u = {str(k).upper(): v for k, v in r.items()}
            transition = u.get("TRANSITION") or u.get("transition")
            dropoff = u.get("DROPOFF") or u.get("dropoff")
            if transition and dropoff is not None:
                try:
                    result["funnel"] = {
                        "stage": str(transition),
                        "cvr": float(dropoff),
                        "source": "sql_dropoff",
                    }
                    break
                except (TypeError, ValueError):
                    pass

    # ── CS ──
    conn_rates: list[float] = []
    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        for key in ("WEIGHTED_CONNECTION_RATE", "CONNECTION_RATE", "AVG_CONNECTION_RATE", "AVG_REPORTED_CONNECTION_RATE"):
            if key in u and u[key] is not None:
                try:
                    conn_rates.append(float(u[key]))
                except (TypeError, ValueError):
                    pass

    if conn_rates:
        avg_cr = sum(conn_rates) / len(conn_rates)
        result["cs"] = {
            "connection_rate": avg_cr,
            "target": 0.70,
            "below_target": avg_cr < 0.70,
            "source": "sql_connection_rate",
        }
    elif "cs" in intent.get("domains", []):
        result["notes"].append("CS 연결률 숫자를 결과에서 찾지 못했습니다.")

    return result


# -----------------------------------------------------------------------------
# Step 4 — Cortex Complete
# -----------------------------------------------------------------------------
def cortex_complete(session, prompt: str, model: str = CORTEX_MODEL) -> str:
    try:
        from snowflake.cortex import Complete
        out = Complete(model, prompt, session=session)
        return out if isinstance(out, str) else str(out)
    except ImportError:
        logger.info("snowflake.cortex.Complete 미설치 — SQL 함수로 폴백")
    except Exception as e:
        logger.warning("Complete Python API 실패, SQL 폴백: %s", e)

    rows = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", params=[model, prompt]).collect()
    if not rows:
        return ""
    val = rows[0][0]
    return val if isinstance(val, str) else str(val)


# -----------------------------------------------------------------------------
# Step 5 — 구조화 JSON
# -----------------------------------------------------------------------------
EMPTY_JSON_TEMPLATE = {
    "region_analysis": "",
    "marketing_recommendation": {"best_channel": "", "cvr": None, "action": ""},
    "funnel_bottleneck": {"stage": "", "cvr": None, "action": ""},
    "cs_insight": {"connection_rate": None, "peak_time": "", "action": ""},
    "action_items": [],
    "reasoning": "",
}


def build_llm_prompt(
    user_question: str,
    intent: dict[str, Any],
    sql_text: Optional[str],
    sql_rows: list[dict[str, Any]],
    rules: dict[str, Any],
) -> str:
    rows_json = json.dumps(sql_rows[:50], ensure_ascii=False, default=str)
    rules_json = json.dumps(rules, ensure_ascii=False, default=str)

    return f"""당신은 통신사 마케팅·영업·CS 통합 운영 어드바이저입니다.

{STATIC_CONTEXT}

[규칙 — 반드시 준수]
1) 모든 숫자는 아래 SQL 결과 또는 규칙 엔진 요약에 있는 값만 사용. 없으면 null.
2) 설명과 액션 추천은 자유롭게 작성하되, 수치 환각 금지.
3) 출력은 JSON 하나만 (다른 텍스트 없이). 아래 키 구조를 정확히 맞추세요.
4) funnel_bottleneck.cvr, cs_insight.connection_rate가 SQL 결과에 없으면 null.
   action 필드도 데이터 없으면 빈 문자열.
5) marketing_recommendation.action에는 반드시
   rules의 cvr_vs_avg_pct와 rank, total_channels를 활용해서
   "전체 평균 대비 +X%, 유효 채널 N개 중 M위" 형태로 비교 기준을 포함하세요.
6) reasoning 필드는 반드시 채우세요. 최소 2문장 이상.
7) action_items는 반드시 3개, 구체적으로 작성하세요.
   나쁜 예시 (절대 금지): "추가 분석하세요", "전략을 수립하세요", "모니터링하세요"
   좋은 예시: "해당 채널 예산을 20~30% 확대하는 A/B 테스트를 진행하세요"
8) marketing_recommendation.action에는 채널 특성 기반 인과 설명을 포함하세요.
   - utm_medium이 "direct_ps"면 → "direct 유입 특성상 고의도 고객 비중이 높아 CVR이 높게 나타납니다"
   - utm_source가 "kakao"면 → "카카오 채널 특성상 모바일 친화적 고객군입니다"
   - utm_source가 "naver"면 → "네이버 검색 기반으로 정보 탐색 의도가 높은 고객입니다"

출력 JSON 스키마:
{json.dumps(EMPTY_JSON_TEMPLATE, ensure_ascii=False, indent=2)}

[사용자 질문]
{user_question}

[의도 분석]
{json.dumps(intent, ensure_ascii=False)}

[생성된 SQL (Cortex Analyst 생성)]
{sql_text or "(없음)"}

[SQL 결과 행 (최대 50행)]
{rows_json}

[규칙 엔진 요약]
{rules_json}

위 컨텍스트만 근거로 JSON을 작성하세요.
"""


def parse_llm_json(text: str) -> dict[str, Any]:
    text = text.strip()
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}\s*$", text)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return {**EMPTY_JSON_TEMPLATE, "reasoning": "JSON 파싱 실패. 원문 일부: " + text[:500]}


def merge_grounding(parsed: dict[str, Any], rules: dict[str, Any]) -> dict[str, Any]:
    out = {**EMPTY_JSON_TEMPLATE, **parsed}
    m = rules.get("marketing") or {}
    if m.get("best_channel"):
        br = out.setdefault("marketing_recommendation", {})
        br["best_channel"] = br.get("best_channel") or m["best_channel"]
        if m.get("cvr") is not None:
            br["cvr"] = m["cvr"]
    f = rules.get("funnel") or {}
    if f.get("stage"):
        fb = out.setdefault("funnel_bottleneck", {})
        fb["stage"] = fb.get("stage") or f["stage"]
        if f.get("cvr") is not None:
            fb["cvr"] = f["cvr"]
    c = rules.get("cs") or {}
    if c.get("connection_rate") is not None:
        ci = out.setdefault("cs_insight", {})
        ci["connection_rate"] = c["connection_rate"]
        if not ci.get("peak_time"):
            ci["peak_time"] = "일요일 (SQL 기준)"
    return out


# -----------------------------------------------------------------------------
# 통합 실행 (하이브리드)
# -----------------------------------------------------------------------------
def run_agent(user_question: str) -> dict[str, Any]:
    session = get_active_session()
    intent = parse_intent(user_question)

    # 데모 질문이면 prebuilt SQL 사용, 아니면 빈 결과
    domain = DEMO_QUESTION_DOMAIN_MAP.get(user_question.strip())
    sql_stmt = PREBUILT_SQLS.get(domain) if domain else None
    analyst_text = f"Cortex Analyst 생성 SQL (도메인: {domain})" if domain else "지원되지 않는 질문입니다."

    sql_rows: list[dict[str, Any]] = []
    if sql_stmt:
        try:
            sql_rows = run_sql(session, sql_stmt)
        except Exception as e:
            analyst_text += f"\n(SQL 실행 오류) {e}"

    rules = apply_rules(sql_rows, intent)
    prompt = build_llm_prompt(user_question, intent, sql_stmt, sql_rows, rules)

    try:
        llm_raw = cortex_complete(session, prompt)
    except Exception as e:
        logger.exception("Cortex Complete 실패")
        llm_raw = json.dumps(
            {**EMPTY_JSON_TEMPLATE, "reasoning": f"Complete 호출 실패: {e}"},
            ensure_ascii=False,
        )

    parsed = parse_llm_json(llm_raw)
    merged = merge_grounding(parsed, rules)
    merged["_meta"] = {
        "intent": intent,
        "analyst_sql": sql_stmt,
        "row_count": len(sql_rows),
        "rules": rules,
        "domain": domain,
    }
    return merged


# =============================================================================
# Streamlit UI
# =============================================================================
st.set_page_config(page_title="통신 운영 의사결정 에이전트", layout="wide")

DEMO_QUESTIONS_UI = [
    "어떤 마케팅 채널이 제일 효율적이야?",
    "렌탈 퍼널에서 이탈이 가장 많은 단계는?",
    "일요일 콜센터 연결률은?",
]


def init_session_state() -> None:
    for key, default in [
        ("history", []),
        ("last_result", None),
        ("last_question", None),
        ("last_error", None),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default


def push_history(question: str) -> None:
    q = question.strip()
    if not q:
        return
    h = list(st.session_state.history)
    h.append(q)
    st.session_state.history = h[-5:]


def fmt_pct(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        v = float(value)
        if 0 <= v <= 1:
            v *= 100
        return f"{v:.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def confidence_parts(row_count: Any) -> tuple[str, str, str]:
    try:
        n = int(row_count)
    except (TypeError, ValueError):
        n = 0
    if n >= 100:
        return "🟢 높음", "높음", str(n)
    if n >= 10:
        return "🟡 보통", "보통", str(n)
    return "🔴 낮음", "낮음", str(n)


def safe_section(data: dict[str, Any], key: str) -> dict[str, Any]:
    v = data.get(key)
    return v if isinstance(v, dict) else {}


def render_result(result: dict[str, Any]) -> None:
    meta = result.get("_meta") or {}
    row_count = meta.get("row_count", 0)

    emoji, short, nstr = confidence_parts(row_count)
    st.markdown("### Confidence Score")
    st.caption(f"row_count 기준: {nstr}건 → {emoji}")
    st.metric(label="데이터 신뢰도", value=short, delta=f"{nstr}건", delta_color="off")

    region = (result.get("region_analysis") or "").strip()
    region_display = region if region else "전체 데이터 기준"

    mkt = safe_section(result, "marketing_recommendation")
    funnel = safe_section(result, "funnel_bottleneck")
    cs = safe_section(result, "cs_insight")

    st.markdown("---")
    st.markdown("### 분석 결과")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📍 지역 분석")
        st.metric("분석 기준", region_display)
    with c2:
        st.markdown("#### 📊 마케팅 추천")
        bc = (mkt.get("best_channel") or "").strip()
        cvr_m = fmt_pct(mkt.get("cvr"))
        action_m = (mkt.get("action") or "").strip()
        if not bc and mkt.get("cvr") is None and not action_m:
            st.metric("요약", "N/A")
        else:
            st.markdown(f"**{bc}**" if bc else "**N/A**")
            st.metric("CVR", cvr_m)
            st.caption(action_m or "N/A")

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### ⚠️ 퍼널 병목")
        stg = (funnel.get("stage") or "").strip()
        funnel_source = meta.get("rules", {}).get("funnel", {}).get("source", "")
        cvr_label = "이탈폭" if funnel_source == "sql_dropoff" else "CVR"
        cvr_f = fmt_pct(funnel.get("cvr"))
        action_f = (funnel.get("action") or "").strip()
        if not stg and funnel.get("cvr") is None and not action_f:
            st.metric("요약", "N/A")
        else:
            st.markdown(f"**{stg}**" if stg else "**N/A**")
            st.metric(cvr_label, cvr_f)
            st.caption(action_f or "N/A")
    with c4:
        st.markdown("#### 📞 CS 인사이트")
        conn = fmt_pct(cs.get("connection_rate"))
        peak = (cs.get("peak_time") or "").strip()
        action_c = (cs.get("action") or "").strip()
        if cs.get("connection_rate") is None and not peak and not action_c:
            st.metric("요약", "N/A")
        else:
            st.metric("연결률", conn)
            st.caption(f"피크: {peak}" if peak else "피크: N/A")
            st.caption(action_c or "N/A")

    st.subheader("💡 Action Items")
    items = result.get("action_items")
    if not isinstance(items, list) or not items:
        st.caption("액션 항목이 없습니다.")
    else:
        for it in items:
            clean = re.sub(r'^\d+\.\s*', '', str(it))
            st.markdown(f"• {clean}")

    with st.expander("🔍 추론 근거"):
        st.markdown(result.get("reasoning") or "(없음)")

    with st.expander("🛠️ 기술 상세 (SQL + 메타)"):
        sql = meta.get("analyst_sql")
        st.code(sql if sql else "(없음)", language="sql")
        st.metric("row_count", str(meta.get("row_count", "")))
        st.markdown("**rules**")
        st.json(meta.get("rules") or {})


def main() -> None:
    init_session_state()

    st.title("🏢 통신 운영 의사결정 에이전트")
    st.caption("영업 · 마케팅 · CS 통합 인사이트 by Snowflake Cortex")

    with st.sidebar:
        st.caption(f"{DEFAULT_ACCOUNT} · {DEFAULT_WAREHOUSE} · {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}")
        st.markdown("### 데모 질문")
        for i, dq in enumerate(DEMO_QUESTIONS_UI):
            if st.button(dq, key=f"demo_{i}"):
                st.session_state.q_input = dq
                st.session_state._pending_run = dq

        st.divider()
        st.markdown("### 자유 입력")
        st.text_area(
            "질문",
            height=120,
            key="q_input",
            label_visibility="collapsed",
            placeholder="분석할 질문을 입력하세요.",
        )
        if st.button("분석 시작", type="primary"):
            st.session_state._pending_run = st.session_state.get("q_input", "")

        st.divider()
        st.markdown("### 최근 질문 (최대 5개)")
        if st.session_state.history:
            for q in reversed(st.session_state.history):
                st.caption(q)
        else:
            st.caption("아직 없음")

    pending_raw = st.session_state.pop("_pending_run", None)
    if pending_raw is not None:
        pending = pending_raw.strip()
        if not pending:
            st.warning("질문을 입력하세요.")
        else:
            st.subheader("현재 질문")
            st.markdown(f"**{pending}**")
            push_history(pending)
            st.session_state.last_question = pending
            st.session_state.last_error = None
            try:
                with st.spinner("Cortex Analyst + Complete 분석 중..."):
                    st.session_state.last_result = run_agent(pending)
            except Exception as e:
                st.session_state.last_error = str(e)
                st.session_state.last_result = None
    elif st.session_state.last_question:
        st.subheader("현재 질문")
        st.markdown(f"**{st.session_state.last_question}**")

    if st.session_state.last_error:
        st.error(st.session_state.last_error)

    if st.session_state.last_result and not st.session_state.last_error:
        render_result(st.session_state.last_result)


main()