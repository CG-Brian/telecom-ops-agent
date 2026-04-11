"""Streamlit 대시보드: agent_v2 통합 (멀티 도메인 + 통합 사고)"""

from __future__ import annotations

import re
from typing import Any

import streamlit as st

import agent_v2 as agent

st.set_page_config(
    page_title="통신 운영 의사결정 에이전트",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* 전체 폰트 */
html, body, [class*="css"] { font-family: 'Pretendard', 'Noto Sans KR', sans-serif; }

/* 결론 블록 */
.conclusion-box {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border-left: 4px solid #e94560;
    border-radius: 8px;
    padding: 1.2rem 1.5rem;
    color: #f5f5f5;
    margin: 0.8rem 0;
    font-size: 1.05rem;
    line-height: 1.7;
}

/* 도메인 카드 */
.domain-card {
    background: #f8f9fb;
    border-radius: 10px;
    padding: 1rem 1.2rem;
    border: 1px solid #e8eaed;
    height: 100%;
}

/* 우선순위 아이템 */
.priority-row {
    display: flex;
    align-items: flex-start;
    gap: 0.8rem;
    padding: 0.6rem 0;
    border-bottom: 1px solid #f0f0f0;
}
.priority-rank {
    background: #e94560;
    color: white;
    border-radius: 50%;
    width: 26px;
    height: 26px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.78rem;
    font-weight: 700;
    flex-shrink: 0;
    margin-top: 2px;
}
.priority-rank.rank2 { background: #f47340; }
.priority-rank.rank3 { background: #3a86ff; }

/* confidence 배지 */
.badge-high   { background:#d1fae5; color:#065f46; border-radius:20px; padding:3px 12px; font-size:0.82rem; font-weight:600; }
.badge-medium { background:#fef3c7; color:#92400e; border-radius:20px; padding:3px 12px; font-size:0.82rem; font-weight:600; }
.badge-low    { background:#fee2e2; color:#991b1b; border-radius:20px; padding:3px 12px; font-size:0.82rem; font-weight:600; }

/* action 아이템 */
.action-item {
    background: #f0f7ff;
    border-left: 3px solid #3a86ff;
    border-radius: 0 6px 6px 0;
    padding: 0.55rem 0.9rem;
    margin: 0.35rem 0;
    font-size: 0.93rem;
}

/* 빈 화면 온보딩 */
.onboarding {
    text-align: center;
    padding: 3rem 1rem;
    color: #888;
}
.onboarding h3 { color: #444; margin-bottom: 0.5rem; }

/* metric 레이블 */
[data-testid="stMetricLabel"] { font-size: 0.78rem !important; }
</style>
""", unsafe_allow_html=True)

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
    if not h or h[-1] != q:
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


def render_confidence(confidence: str) -> None:
    badge_map = {
        "높음": ('<span class="badge-high">🟢 신뢰도 높음</span>', None),
        "보통": ('<span class="badge-medium">🟡 신뢰도 보통</span>', None),
        "낮음": ('<span class="badge-low">🔴 신뢰도 낮음</span>', None),
    }
    if confidence in badge_map:
        html, _ = badge_map[confidence]
        st.markdown(html, unsafe_allow_html=True)


def render_result(result: dict[str, Any]) -> None:
    meta = result.get("_meta") or {}
    evidence = meta.get("evidence") or {}
    conflicts = meta.get("conflicts") or {}
    priorities = meta.get("priorities") or []
    intent = meta.get("intent") or {}

    # ── 상단 헤더 행: 신뢰도 + 의사결정 유형 ──────────────────────────
    col_conf, col_type, col_domains = st.columns([2, 2, 3])
    with col_conf:
        render_confidence(result.get("confidence", ""))
    with col_type:
        dtype = intent.get("decision_type", "")
        if dtype:
            st.caption(f"의사결정 유형: **{dtype}**")
    with col_domains:
        domains = intent.get("domains", [])
        if domains:
            label_map = {"marketing": "📊 마케팅", "funnel": "⚠️ 퍼널", "cs": "📞 CS", "sales": "📍 영업"}
            tags = " · ".join(label_map.get(d, d) for d in domains)
            st.caption(f"분석 도메인: {tags}")

    # ── 통합 결론 ──────────────────────────────────────────────────────
    conclusion = result.get("conclusion", "")
    if conclusion:
        st.markdown("#### 🔥 통합 결론")
        st.markdown(f'<div class="conclusion-box">{conclusion}</div>', unsafe_allow_html=True)

    # ── Conflict 해석 ─────────────────────────────────────────────────
    interpretation = conflicts.get("interpretation", "")
    if interpretation:
        st.info(f"💡 {interpretation}")

    # ── 3-열 레이아웃: 원인 / 우선순위 / 액션 ─────────────────────────
    col_cause, col_prio, col_action = st.columns([1, 1, 1])

    with col_cause:
        direct = result.get("direct_cause", "")
        indirect = result.get("indirect_cause", "")
        if direct or indirect:
            st.markdown("##### 🎯 원인 분석")
            if direct:
                st.markdown("**직접 원인**")
                st.markdown(direct)
            if indirect:
                st.markdown("**간접 원인**")
                st.markdown(indirect or "데이터 제한적")

    with col_prio:
        if priorities:
            st.markdown("##### 📋 우선순위")
            rank_classes = ["", "rank1", "rank2", "rank3"]
            for p in priorities[:3]:
                rank = p.get("rank", "")
                area = p.get("area", "")
                action = p.get("action", "")
                impact = p.get("impact", "")
                rank_cls = rank_classes[int(rank)] if str(rank).isdigit() and int(rank) <= 3 else ""
                st.markdown(
                    f'<div class="priority-row">'
                    f'<div class="priority-rank {rank_cls}">{rank}</div>'
                    f'<div><strong>[{area}]</strong> {action}<br>'
                    f'<small style="color:#888">영향도: {impact}</small></div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    with col_action:
        items = result.get("action_items")
        if isinstance(items, list) and items:
            st.markdown("##### 💡 액션 아이템")
            for it in items:
                clean = re.sub(r"^\d+\.\s*", "", str(it))
                st.markdown(f'<div class="action-item">{clean}</div>', unsafe_allow_html=True)

    # ── 도메인별 Evidence 카드 ─────────────────────────────────────────
    available = {k: v for k, v in evidence.items() if isinstance(v, dict) and v.get("available")}
    if available:
        st.markdown("---")
        st.markdown("#### 📊 도메인별 분석 근거")
        cols = st.columns(len(available))
        label_map = {
            "marketing": "📊 마케팅",
            "funnel": "⚠️ 퍼널",
            "cs": "📞 CS",
            "sales": "📍 영업",
        }
        for i, (domain, ev) in enumerate(available.items()):
            with cols[i]:
                sig = signal_badge(ev.get("signal", "neutral"))
                label = label_map.get(domain, domain)
                st.markdown(
                    f'<div class="domain-card"><strong>{sig} {label}</strong></div>',
                    unsafe_allow_html=True,
                )
                if domain == "marketing":
                    st.metric("CVR", f"{ev.get('cvr', 'N/A')}%")
                    st.caption(f"채널: {ev.get('best_channel', '')}")
                    st.caption(f"평균 대비: +{ev.get('cvr_vs_avg_pct', '')}%")
                    st.caption(f"순위: {ev.get('rank', '')}위 / {ev.get('total_channels', '')}개 채널")
                elif domain == "funnel":
                    st.metric("병목 단계", ev.get("bottleneck_stage", "N/A"))
                    st.caption(f"CVR: {ev.get('bottleneck_cvr', '')}%")
                    if ev.get("max_drop_stage"):
                        st.caption(f"최대 이탈: {ev.get('max_drop_stage')} {ev.get('max_drop_value', '')}%p")
                elif domain == "cs":
                    st.metric("연결률", f"{ev.get('connection_rate', 'N/A')}%")
                    below = ev.get("below_target")
                    st.caption(f"목표: 70% | {'미달 ❌' if below else '달성 ✅'}")
                    st.caption(f"최저 시간대: {ev.get('peak_time', '')}")
                elif domain == "sales":
                    st.metric("계약", f"{ev.get('total_contracts', 0):,}건")
                    st.caption(f"매출: {int(ev.get('total_revenue', 0)):,}원")

    # ── 추론 근거 / 기술 상세 (접기) ──────────────────────────────────
    st.markdown("")
    reasoning = result.get("reasoning", "")
    exp1, exp2 = st.columns(2)
    with exp1:
        if reasoning:
            with st.expander("🔍 추론 근거 보기"):
                st.markdown(reasoning)
    with exp2:
        with st.expander("🛠️ 기술 상세 보기"):
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


def render_onboarding() -> None:
    st.markdown(
        '<div class="onboarding">'
        "<h3>🏢 통신 운영 의사결정 에이전트</h3>"
        "<p>마케팅 · 영업 · CS 데이터를 통합해 하나의 의사결정을 내립니다.</p>"
        "<p style='margin-top:1rem;font-size:0.9rem;'>← 왼쪽 사이드바에서 예시 질문을 선택하거나 직접 입력해보세요.</p>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    for col, title, desc in [
        (c1, "📊 마케팅 채널 최적화", "1,464개 채널 중 예산을 집중할 채널을 자동 선택"),
        (c2, "⚠️ 퍼널 병목 탐지",    "등록 → 활성화 구간의 이탈 원인을 자동 진단"),
        (c3, "📞 CS 운영 최적화",     "시간대별 연결률을 분석해 인력 배치를 제안"),
    ]:
        with col:
            st.info(f"**{title}**\n\n{desc}")


def main() -> None:
    init_session_state()

    # ── 사이드바 ─────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## 🏢 의사결정 에이전트")
        st.caption("Snowflake Cortex 기반 멀티 도메인 분석")
        st.divider()

        st.markdown("### 💬 예시 질문")
        for i, dq in enumerate(DEMO_QUESTIONS):
            if st.button(dq, key=f"demo_{i}", use_container_width=True):
                st.session_state.q_input = dq
                st.session_state._pending_run = dq

        st.divider()
        st.markdown("### ✏️ 직접 질문하기")
        st.text_area(
            "질문",
            height=110,
            key="q_input",
            label_visibility="collapsed",
            placeholder="예: 이번 달 CS 연결률이 왜 낮아?",
        )
        if st.button("🔍 분석 시작", type="primary", use_container_width=True):
            st.session_state._pending_run = st.session_state.get("q_input", "")

        if st.session_state.history:
            st.divider()
            st.markdown("### 🕘 최근 질문")
            for q in reversed(st.session_state.history):
                if st.button(q, key=f"hist_{q[:20]}", use_container_width=True):
                    st.session_state.q_input = q
                    st.session_state._pending_run = q

    # ── 메인 화면 ────────────────────────────────────────────────────
    pending_raw = st.session_state.pop("_pending_run", None)
    if pending_raw is not None:
        pending = pending_raw.strip()
        if not pending:
            st.warning("질문을 입력하세요.")
        else:
            push_history(pending)
            st.session_state.last_question = pending
            st.session_state.last_error = None
            try:
                with st.spinner("멀티 도메인 분석 중... 잠시만 기다려주세요."):
                    st.session_state.last_result = agent.run_agent(pending)
            except Exception as e:
                st.session_state.last_error = str(e)
                st.session_state.last_result = None

    if st.session_state.last_question:
        st.markdown(f"### 🔎 {st.session_state.last_question}")
        st.markdown("---")

    if st.session_state.last_error:
        st.error(f"오류 발생: {st.session_state.last_error}")
    elif st.session_state.last_result:
        render_result(st.session_state.last_result)
    else:
        render_onboarding()


if __name__ == "__main__":
    main()
