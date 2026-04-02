"""Streamlit 대시보드: agent.run_agent() 통합."""

from __future__ import annotations

from typing import Any

import streamlit as st

import agent

st.set_page_config(
    page_title="통신 운영 의사결정 에이전트",
    layout="wide",
)

DEMO_QUESTIONS = [
    "어떤 마케팅 채널이 제일 효율적이야?",
    "렌탈 퍼널에서 이탈이 가장 많은 단계는?",
    "일요일 콜센터 연결률은?",
]


def init_session_state() -> None:
    if "history" not in st.session_state:
        st.session_state.history = []
    if "last_result" not in st.session_state:
        st.session_state.last_result = None
    if "last_question" not in st.session_state:
        st.session_state.last_question = None
    if "last_error" not in st.session_state:
        st.session_state.last_error = None


def push_history(question: str) -> None:
    q = question.strip()
    if not q:
        return
    h = list(st.session_state.history)
    h.append(q)
    st.session_state.history = h[-5:]


def fmt_cvr(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        v = float(value)
        if 0 <= v <= 1:
            v *= 100
        return f"{v:.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def fmt_connection_rate(value: Any) -> str:
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
    if not isinstance(meta, dict):
        meta = {}
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
        cvr_m = fmt_cvr(mkt.get("cvr"))
        action_m = (mkt.get("action") or "").strip()
        mkt_empty = not bc and mkt.get("cvr") is None and not action_m
        if mkt_empty:
            st.metric("요약", "N/A")
        else:
            st.markdown(f"**{bc}**" if bc else "**N/A**")
            st.metric("CVR", cvr_m)
            st.caption(action_m if action_m else "N/A")

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### ⚠️ 퍼널 병목")
        stg = (funnel.get("stage") or "").strip()
        cvr_f = fmt_cvr(funnel.get("cvr"))
        action_f = (funnel.get("action") or "").strip()
        funnel_empty = not stg and funnel.get("cvr") is None and not action_f
        if funnel_empty:
            st.metric("요약", "N/A")
        else:
            st.markdown(f"**{stg}**" if stg else "**N/A**")
            st.metric("CVR", cvr_f)
            st.caption(action_f if action_f else "N/A")
    with c4:
        st.markdown("#### 📞 CS 인사이트")
        conn = fmt_connection_rate(cs.get("connection_rate"))
        peak = (cs.get("peak_time") or "").strip()
        action_c = (cs.get("action") or "").strip()
        cs_empty = cs.get("connection_rate") is None and not peak and not action_c
        if cs_empty:
            st.metric("요약", "N/A")
        else:
            st.metric("연결률", conn)
            st.caption(f"피크: {peak}" if peak else "피크: N/A")
            st.caption(action_c if action_c else "N/A")

    st.subheader("💡 Action Items")
    items = result.get("action_items")
    if not isinstance(items, list):
        items = []
    if not items:
        st.caption("액션 항목이 없습니다.")
    else:
        for i, it in enumerate(items, 1):
            st.markdown(f"{i}. {it}")

    with st.expander("🔍 추론 근거"):
        st.markdown(result.get("reasoning") or "(없음)")

    with st.expander("🛠️ 기술 상세 (SQL + 메타)"):
        sql = meta.get("analyst_sql")
        st.code(sql if sql else "(없음)", language="sql")
        st.metric("row_count", str(meta.get("row_count", "")))
        st.markdown("**rules**")
        st.json(meta.get("rules") if meta.get("rules") is not None else {})


def main() -> None:
    init_session_state()

    st.title("🏢 통신 운영 의사결정 에이전트")
    st.caption("영업 · 마케팅 · CS 통합 인사이트 by Snowflake Cortex")

    with st.sidebar:
        st.markdown("### 데모 질문")
        for i, dq in enumerate(DEMO_QUESTIONS):
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
                    st.session_state.last_result = agent.run_agent(pending)
            except Exception as e:  # noqa: BLE001
                st.session_state.last_error = str(e)
                st.session_state.last_result = None
    elif st.session_state.last_question:
        st.subheader("현재 질문")
        st.markdown(f"**{st.session_state.last_question}**")

    if st.session_state.last_error:
        st.error(st.session_state.last_error)

    if st.session_state.last_result and not st.session_state.last_error:
        render_result(st.session_state.last_result)


if __name__ == "__main__":
    main()
