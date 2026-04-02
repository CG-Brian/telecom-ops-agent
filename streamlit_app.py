"""Streamlit 대시보드: agent_v2 통합 (멀티 도메인 + 통합 사고)"""

from __future__ import annotations

import re
from typing import Any, Optional

import streamlit as st

import agent_v2 as agent

st.set_page_config(
    page_title="통신 운영 의사결정 에이전트",
    layout="wide",
)

DEMO_QUESTIONS = [
    "전환율이 낮은 이유가 뭐야?",
    "지금 성과를 올리려면 뭐부터 해야 해?",
    "어떤 마케팅 채널이 제일 효율적이야?",
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


def signal_badge(signal: str) -> str:
    return {"good": "🟢", "neutral": "🟡", "bad": "🔴"}.get(signal, "⚪")


def render_result(result: dict[str, Any]) -> None:
    meta = result.get("_meta") or {}
    evidence = meta.get("evidence") or {}
    conflicts = meta.get("conflicts") or {}
    priorities = meta.get("priorities") or []
    intent = meta.get("intent") or {}

    # ── Confidence ──
    confidence = result.get("confidence", "")
    if confidence == "높음":
        st.success(f"🟢 High Confidence")
    elif confidence == "보통":
        st.warning(f"🟡 Medium Confidence")
    elif confidence == "낮음":
        st.error(f"🔴 Low Confidence")

    # ── 결론 (최상단) ──
    conclusion = result.get("conclusion", "")
    if conclusion:
        st.markdown("---")
        st.markdown("## 🔥 통합 결론")
        st.markdown(f"> {conclusion}")

    # ── Conflict 해석 ──
    interpretation = conflicts.get("interpretation", "")
    if interpretation:
        st.info(f"💡 {interpretation}")

    # ── Action Items ──
    items = result.get("action_items")
    if isinstance(items, list) and items:
        st.markdown("---")
        st.markdown("## 💡 Action Items")
        for it in items:
            clean = re.sub(r"^\d+\.\s*", "", str(it))
            st.markdown(f"• {clean}")

    # ── 우선순위 ──
    if priorities:
        st.markdown("---")
        st.markdown("### 📋 우선순위")
        for p in priorities:
            rank = p.get("rank", "")
            area = p.get("area", "")
            action = p.get("action", "")
            impact = p.get("impact", "")
            st.markdown(f"**{rank}순위** [{area}] {action} *(영향도: {impact})*")

    # ── 직접/간접 원인 ──
    direct = result.get("direct_cause", "")
    indirect = result.get("indirect_cause", "")
    if direct or indirect:
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 🎯 직접 원인")
            st.markdown(direct or "데이터 제한적")
        with col2:
            st.markdown("#### 🔗 간접 원인")
            st.markdown(indirect or "데이터 제한적")

    # ── 도메인별 Evidence ──
    available = {k: v for k, v in evidence.items() if isinstance(v, dict) and v.get("available")}
    if available:
        st.markdown("---")
        st.markdown("### 📊 도메인별 분석 근거")
        cols = st.columns(len(available))
        for i, (domain, ev) in enumerate(available.items()):
            with cols[i]:
                sig = signal_badge(ev.get("signal", "neutral"))
                label = {"marketing": "📊 마케팅", "funnel": "⚠️ 퍼널", "cs": "📞 CS", "sales": "📍 영업"}.get(domain, domain)
                st.markdown(f"#### {sig} {label}")

                if domain == "marketing":
                    st.metric("CVR", f"{ev.get('cvr', 'N/A')}%")
                    st.caption(f"채널: {ev.get('best_channel', '')}")
                    st.caption(f"평균 대비: +{ev.get('cvr_vs_avg_pct', '')}%")
                    st.caption(f"순위: {ev.get('rank', '')}위/{ev.get('total_channels', '')}개")

                elif domain == "funnel":
                    st.metric("병목 단계", ev.get("bottleneck_stage", "N/A"))
                    st.caption(f"CVR: {ev.get('bottleneck_cvr', '')}%")
                    if ev.get("max_drop_stage"):
                        st.caption(f"최대 이탈: {ev.get('max_drop_stage')} {ev.get('max_drop_value', '')}%p")

                elif domain == "cs":
                    st.metric("연결률", f"{ev.get('connection_rate', 'N/A')}%")
                    st.caption(f"목표: 70% | {'미달 ❌' if ev.get('below_target') else '달성 ✅'}")
                    st.caption(f"최저 시간대: {ev.get('peak_time', '')}")

                elif domain == "sales":
                    st.metric("계약", f"{ev.get('total_contracts', 0):,}건")
                    st.caption(f"매출: {int(ev.get('total_revenue', 0)):,}원")

    # ── 추론 근거 접기 ──
    reasoning = result.get("reasoning", "")
    if reasoning:
        with st.expander("🔍 추론 근거"):
            st.markdown(reasoning)

    with st.expander("🛠️ 기술 상세"):
        st.markdown(f"**의사결정 유형:** {intent.get('decision_type', '')}")
        st.markdown(f"**분석 도메인:** {', '.join(intent.get('domains', []))}")
        sql = meta.get("analyst_sql")
        st.code(sql if sql else "(없음)", language="sql")
        st.markdown("**Evidence:**")
        st.json(evidence)
        if conflicts.get("conflicts"):
            st.markdown("**Conflicts:**")
            for c in conflicts["conflicts"]:
                st.markdown(f"- {c}")


def main() -> None:
    init_session_state()

    st.title("🏢 통신 운영 의사결정 에이전트")
    st.caption("영업 · 마케팅 · CS 통합 인사이트 by Snowflake Cortex")

    with st.sidebar:
        st.markdown("### 💬 예시 질문")
        st.caption("자유롭게 질문하세요")
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
            placeholder="예: 전환율이 낮은 이유가 뭐야?",
        )
        if st.button("분석 시작", type="primary"):
            st.session_state._pending_run = st.session_state.get("q_input", "")

        st.divider()
        st.markdown("### 최근 질문")
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
                with st.spinner("멀티 도메인 분석 중..."):
                    st.session_state.last_result = agent.run_agent(pending)
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


if __name__ == "__main__":
    main()