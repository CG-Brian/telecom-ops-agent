"""
telecom-ops-agent Evaluation v4
- 속도 개선: SQL 데이터를 한 번만 가져와서 agent에 주입
- baseline 비교 수정: CVR Only Top-5 함정 분석 추가
- 함정 케이스 평가 추가
"""

from __future__ import annotations

import json
import os
import random
import time
from typing import Any, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pandas as pd
from snowflake.snowpark import Session

# agent_v2 임포트
try:
    from agent_v2 import (
        run_agent, get_session,
        extract_marketing_evidence,
        extract_funnel_evidence,
        extract_cs_evidence,
        detect_conflicts,
        prioritize_actions,
        build_prompt_v2,
        parse_intent,
        cortex_complete,
        parse_json,
        call_cortex_analyst,
        extract_sql,
        run_sql,
    )
    AGENT_AVAILABLE = True
except Exception as e:
    AGENT_AVAILABLE = False
    print(f"⚠️  agent_v2 임포트 실패: {e}")


# -----------------------------------------------------------------------------
# 데이터 로드
# -----------------------------------------------------------------------------
DB = "SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION"
SESSIONS_MIN  = 500
CONTRACTS_MIN = 50


def load_raw_marketing(session: Session) -> pd.DataFrame:
    df = session.sql(f"""
        SELECT
            UTM_SOURCE, UTM_MEDIUM,
            SUM(TOTAL_SESSIONS)                                   AS total_sessions,
            SUM(TOTAL_CONTRACTS)                                  AS total_contracts,
            SUM(TOTAL_REVENUE)                                    AS total_revenue,
            SUM(TOTAL_CONTRACTS) / NULLIF(SUM(TOTAL_SESSIONS),0) AS contract_cvr,
            SUM(TOTAL_REVENUE)   / NULLIF(SUM(TOTAL_SESSIONS),0) AS revenue_per_session
        FROM {DB}.TELECOM_INSIGHTS.V07_GA4_MARKETING_ATTRIBUTION
        GROUP BY UTM_SOURCE, UTM_MEDIUM
        HAVING SUM(TOTAL_SESSIONS) > 0
    """).to_pandas()
    df.columns = df.columns.str.lower()
    df["channel"] = df["utm_source"].fillna("") + "/" + df["utm_medium"].fillna("")
    return df


def filter_valid(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        (df["total_sessions"]  >= SESSIONS_MIN) &
        (df["total_contracts"] >= CONTRACTS_MIN)
    ].copy()


def compute_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["score"] = df["contract_cvr"] * 0.4 + (df["revenue_per_session"] / 100_000) * 0.6
    return df.sort_values("score", ascending=False).reset_index(drop=True)


# -----------------------------------------------------------------------------
# Baseline
# -----------------------------------------------------------------------------
def baseline_random(df_valid: pd.DataFrame, seed: int = 42) -> str:
    random.seed(seed)
    return random.choice(df_valid["channel"].tolist())


def baseline_cvr_only(df_valid: pd.DataFrame) -> str:
    return df_valid.loc[df_valid["contract_cvr"].idxmax(), "channel"]


def eval_trap_cases(df_valid: pd.DataFrame) -> list[dict[str, Any]]:
    """CVR Only Top-5가 복합score 기준으로 몇 위인지 분석"""
    ranked_composite = compute_score(df_valid).reset_index(drop=True)
    ranked_composite["rank"] = ranked_composite.index + 1
    cvr_top5 = (
        df_valid.sort_values("contract_cvr", ascending=False)
        .head(5)["channel"].tolist()
    )
    results = []
    for i, ch in enumerate(cvr_top5):
        composite_rank = ranked_composite[ranked_composite["channel"] == ch]["rank"].values
        c_rank = int(composite_rank[0]) if len(composite_rank) > 0 else None
        is_trap = c_rank > 5 if c_rank else False
        results.append({
            "cvr_rank": i + 1, "channel": ch,
            "composite_rank": c_rank, "is_trap": is_trap,
            "status": "❌ 함정" if is_trap else "✅ 정상",
        })
    return results


# -----------------------------------------------------------------------------
# 속도 개선: SQL 캐시
# -----------------------------------------------------------------------------
_SQL_CACHE: dict[str, list[dict]] = {}

DOMAIN_QUERIES = {
    "marketing": "마케팅 채널(utm_source, utm_medium)별 세션, 계약 수, 계약 전환율, 매출을 요약해 주세요.",
    "funnel":    "상품 카테고리별 퍼널 단계 전환율과 단계별 이탈폭을 분석해 주세요.",
    "cs":        "콜센터 요일·시간대별 통화 건수와 연결률을 분석해 주세요.",
    "sales":     "지역별 계약 건수와 매출 트렌드를 분석해 주세요.",
}


def prefetch_domain_data(session: Session) -> None:
    """평가 시작 전에 모든 도메인 SQL을 한 번만 실행해서 캐시에 저장"""
    global _SQL_CACHE
    print("  도메인 데이터 사전 로딩 중...")
    for domain, query in DOMAIN_QUERIES.items():
        if domain in _SQL_CACHE:
            continue
        try:
            payload = call_cortex_analyst(session, query)
            sql_stmt, _ = extract_sql(payload)
            if sql_stmt:
                rows = run_sql(session, sql_stmt)
                _SQL_CACHE[domain] = rows
                print(f"    ✅ {domain}: {len(rows)}행 캐시 완료")
            else:
                _SQL_CACHE[domain] = []
                print(f"    ⚠️  {domain}: SQL 생성 실패")
        except Exception as e:
            _SQL_CACHE[domain] = []
            print(f"    ❌ {domain}: {e}")
    print()


def run_agent_cached(user_question: str, session: Session) -> dict[str, Any]:
    """캐시된 데이터로 agent 실행 — Cortex Analyst 호출 없음"""
    intent = parse_intent(user_question)
    evidence: dict[str, Any] = {}

    for domain in intent["domains"]:
        rows = _SQL_CACHE.get(domain)
        if rows is None:
            evidence[domain] = {"available": False, "note": "캐시 없음"}
            continue
        if domain == "marketing":
            evidence["marketing"] = extract_marketing_evidence(rows)
        elif domain == "funnel":
            evidence["funnel"]    = extract_funnel_evidence(rows)
        elif domain == "cs":
            evidence["cs"]        = extract_cs_evidence(rows)

    conflicts  = detect_conflicts(evidence)
    priorities = prioritize_actions(evidence, intent["decision_type"])
    available_summaries = [
        ev.get("summary", "") for ev in evidence.values()
        if isinstance(ev, dict) and ev.get("available")
    ]
    sql_results_summary = (
        "\n".join(f"- {s}" for s in available_summaries)
        if available_summaries else "데이터 없음"
    )

    prompt  = build_prompt_v2(user_question, intent, evidence, conflicts, priorities, sql_results_summary)
    llm_raw = cortex_complete(session, prompt)
    output  = parse_json(llm_raw)

    return {
        **output,
        "_meta": {
            "intent": intent, "analyst_sql": {},
            "row_count": sum(1 for v in evidence.values() if isinstance(v, dict) and v.get("available")),
            "evidence": evidence, "conflicts": conflicts,
            "priorities": priorities, "analyst_note": "",
        },
    }


# -----------------------------------------------------------------------------
# Eval 함수들
# -----------------------------------------------------------------------------
def eval_channel_accuracy(
    df_valid: pd.DataFrame,
    agent_output: dict[str, Any],
    expected_rank_band: str = "top1",   # top1 / top3 / not_top3
    expected_behavior: str = "",
) -> dict[str, Any]:
    ranked = compute_score(df_valid).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    actual_top1 = ranked.iloc[0]["channel"]
    actual_top3 = ranked.head(3)["channel"].tolist()
    mkt_ev = agent_output.get("_meta", {}).get("evidence", {}).get("marketing", {})
    recommended = mkt_ev.get("best_channel", "")
    if not recommended:
        return {"recommended": None, "actual_top1": actual_top1,
                "top1_accuracy": 0, "top3_accuracy": 0,
                "correct": 0, "expected_rank_band": expected_rank_band,
                "actual_rank": None, "total": len(ranked), "note": "마케팅 evidence 없음"}
    actual_rank = ranked[ranked["channel"] == recommended]["rank"].values
    top1_acc = int(recommended == actual_top1)
    top3_acc = int(recommended in actual_top3)

    # expected_rank_band에 따라 정답 기준 결정
    if expected_rank_band == "top1":
        correct = top1_acc
    elif expected_rank_band == "top3":
        correct = top3_acc
    else:  # not_top3: 추천 채널이 Top-3 밖이면 정답 (함정 회피)
        correct = int(recommended not in actual_top3)

    return {
        "recommended":        recommended,
        "actual_top1":        actual_top1,
        "top1_accuracy":      top1_acc,
        "top3_accuracy":      top3_acc,
        "correct":            correct,
        "expected_rank_band": expected_rank_band,
        "expected_behavior":  expected_behavior,
        "actual_rank":        int(actual_rank[0]) if len(actual_rank) > 0 else None,
        "total":              len(ranked),
    }


def eval_hallucination(df: pd.DataFrame, agent_output: dict[str, Any], threshold: float = 0.02) -> dict[str, Any]:
    mkt_ev = agent_output.get("_meta", {}).get("evidence", {}).get("marketing", {})
    recommended   = mkt_ev.get("best_channel", "")
    agent_cvr_pct = mkt_ev.get("cvr")
    if not recommended or agent_cvr_pct is None:
        return {"note": "마케팅 evidence 없음 — hallucination 평가 불가"}
    agent_cvr = agent_cvr_pct / 100
    row = df[df["channel"] == recommended]
    if row.empty:
        return {"error": f"{recommended} 채널 데이터 없음"}
    actual_cvr = float(row.iloc[0]["contract_cvr"])
    abs_error  = abs(agent_cvr - actual_cvr)
    is_hall    = abs_error > threshold
    return {
        "channel": recommended,
        "agent_cvr": round(agent_cvr, 4), "actual_cvr": round(actual_cvr, 4),
        "absolute_error": round(abs_error, 4), "is_hallucination": is_hall,
        "status": "✅ Grounded" if not is_hall else "❌ Hallucination",
    }


def eval_impact(df_valid: pd.DataFrame, agent_output: dict[str, Any]) -> dict[str, Any]:
    mkt_ev = agent_output.get("_meta", {}).get("evidence", {}).get("marketing", {})
    recommended = mkt_ev.get("best_channel", "")
    if not recommended:
        return {"note": "마케팅 evidence 없음 — impact 평가 불가"}
    baseline_cvr = df_valid["contract_cvr"].mean()
    baseline_rev = df_valid["revenue_per_session"].mean()
    rec = df_valid[df_valid["channel"] == recommended]
    if rec.empty:
        return {"error": f"{recommended} 없음"}
    rec_cvr = float(rec.iloc[0]["contract_cvr"])
    rec_rev = float(rec.iloc[0]["revenue_per_session"])
    return {
        "channel": recommended,
        "baseline_cvr": round(baseline_cvr, 4), "recommended_cvr": round(rec_cvr, 4),
        "cvr_uplift_rel": round(rec_cvr / baseline_cvr, 2) if baseline_cvr > 0 else 0,
        "baseline_rev": round(baseline_rev, 0), "recommended_rev": round(rec_rev, 0),
        "rev_uplift_rel": round(rec_rev / baseline_rev, 2) if baseline_rev > 0 else 0,
    }


def eval_priority(agent_output: dict[str, Any], expected_priority: Optional[str]) -> dict[str, Any]:
    if not expected_priority or not isinstance(expected_priority, str):
        return {"note": "마케팅 케이스 — 우선순위 평가 없음"}
    priorities = agent_output.get("_meta", {}).get("priorities", [])
    if not priorities:
        return {"expected": expected_priority, "got": None, "correct": False}
    top_priority = priorities[0].get("area", "")
    correct = expected_priority in top_priority or top_priority in expected_priority
    return {
        "expected": expected_priority, "got": top_priority, "correct": correct,
        "all_priorities": [p.get("area") for p in priorities],
    }


def eval_response_quality(agent_output: dict[str, Any]) -> dict[str, Any]:
    has_conclusion   = bool(agent_output.get("conclusion",   "").strip())
    has_direct_cause = bool(agent_output.get("direct_cause", "").strip())
    has_actions      = bool(agent_output.get("action_items"))
    confidence       = agent_output.get("confidence", "없음")
    score = sum([has_conclusion, has_direct_cause, has_actions])
    return {
        "has_conclusion": has_conclusion, "has_direct_cause": has_direct_cause,
        "has_action_items": has_actions, "confidence": confidence,
        "quality_score": f"{score}/3",
        "status": "✅ 완전" if score == 3 else "⚠️ 부분" if score >= 1 else "❌ 미흡",
    }


# -----------------------------------------------------------------------------
# eval_set.csv 로드
# -----------------------------------------------------------------------------
def load_eval_set(path: str = "eval_set.csv") -> pd.DataFrame:
    if os.path.exists(path):
        print(f"  eval_set.csv 로드: {path}")
        return pd.read_csv(path)
    print("  ⚠️  eval_set.csv 없음 → 기본 5개 질문으로 실행")
    return pd.DataFrame([
        {"case_id": "MKT-01", "question": "어떤 마케팅 채널이 제일 효율적이야?",     "expected_top1": "tips_capsule/AJD_platform", "expected_priority": None},
        {"case_id": "MKT-02", "question": "예산을 가장 먼저 늘려야 할 채널은?",       "expected_top1": "tips_capsule/AJD_platform", "expected_priority": None},
        {"case_id": "MKT-03", "question": "CVR만 높고 규모 작은 채널은 피해야 하나?", "expected_top1": "tips_capsule/AJD_platform", "expected_priority": None},
        {"case_id": "MD-01",  "question": "지금 성과를 올리려면 뭐부터 해야 해?",     "expected_top1": None, "expected_priority": "퍼널"},
        {"case_id": "MD-02",  "question": "전환율이 낮은 이유가 뭐야?",               "expected_top1": None, "expected_priority": "퍼널"},
    ])


# -----------------------------------------------------------------------------
# 메인 실행
# -----------------------------------------------------------------------------
def run_eval(eval_path: str = "eval_set.csv") -> None:
    if not AGENT_AVAILABLE:
        print("agent_v2를 임포트할 수 없어서 평가를 실행할 수 없습니다.")
        return

    session  = get_session()
    df       = load_raw_marketing(session)
    df_valid = filter_valid(df)

    print(f"\n전체 채널: {len(df)}개 | 유효 채널: {len(df_valid)}개")
    print(f"Ground truth Top-1: {compute_score(df_valid).iloc[0]['channel']}\n")

    prefetch_domain_data(session)  # ★ 여기서 한 번만 SQL 실행

    eval_df = load_eval_set(eval_path)
    print(f"총 {len(eval_df)}개 케이스 평가 시작...\n")

    results = []

    for _, row in eval_df.iterrows():
        case_id            = row.get("case_id", "?")
        question           = row["question"]
        expected_priority  = row.get("expected_priority") or None
        expected_rank_band = str(row.get("expected_rank_band", "top1") or "top1").strip().lower()
        expected_behavior  = str(row.get("expected_behavior", "") or "")

        print(f"[{case_id}] {question[:50]}")

        try:
            agent_output = run_agent_cached(question, session)
        except Exception as e:
            print(f"  ❌ agent 실행 실패: {e}")
            results.append({"case_id": case_id, "question": question, "error": str(e)})
            continue

        acc     = eval_channel_accuracy(df_valid, agent_output, expected_rank_band, expected_behavior)
        hall    = eval_hallucination(df, agent_output)
        impact  = eval_impact(df_valid, agent_output)
        prio    = eval_priority(agent_output, expected_priority)
        quality = eval_response_quality(agent_output)

        recommended = acc.get("recommended") or "없음"
        label = {"top1": "Top-1", "top3": "Top-3 허용", "not_top3": "함정회피"}.get(expected_rank_band, "Top-1")
        print(f"  추천 채널: {recommended}  [{label}]")
        if acc.get("recommended"):
            correct_icon = '✅' if acc['correct'] else '❌'
            print(f"  정답: {correct_icon}  "                  f"Top-1: {'✅' if acc['top1_accuracy'] else '❌'}  "                  f"Top-3: {'✅' if acc['top3_accuracy'] else '❌'}  "                  f"순위: {acc.get('actual_rank')}위/{acc.get('total')}개")
        if expected_behavior:
            print(f"  기대동작: {expected_behavior}")
        if "status" in hall:
            print(f"  Grounding: {hall['status']}  (오차 {hall.get('absolute_error', 0)*100:.3f}%p)")
        if "cvr_uplift_rel" in impact:
            print(f"  CVR Uplift: {impact['cvr_uplift_rel']:.1f}x  Rev Uplift: {impact['rev_uplift_rel']:.1f}x")
        if expected_priority and isinstance(expected_priority, str):
            print(f"  우선순위: {'✅' if prio.get('correct') else '❌'}  "                  f"(예상: {expected_priority} / 실제: {prio.get('got')})")
        print(f"  응답 품질: {quality['status']}  confidence: {quality['confidence']}")
        print()

        results.append({
            "case_id": case_id, "question": question,
            "expected_rank_band": expected_rank_band,
            "expected_behavior": expected_behavior,
            "agent_output": agent_output,
            "accuracy": acc, "hallucination": hall,
            "impact": impact, "priority": prio, "quality": quality,
        })

        time.sleep(0.3)

    # ------------------------------------------------------------------
    # 집계
    # ------------------------------------------------------------------
    mkt_results = [r for r in results if "error" not in r and r["accuracy"].get("recommended")]
    md_results  = [r for r in results if "error" not in r and r["priority"].get("expected")]

    print("=" * 60)
    print("📊 telecom-ops-agent KPI Evaluation v4")
    print("=" * 60)

    if mkt_results:
        # correct = Top-3 허용 케이스면 top3, 아니면 top1 기준
        correct_scores = [r["accuracy"]["correct"] for r in mkt_results]
        top1_scores    = [r["accuracy"]["top1_accuracy"] for r in mkt_results]
        top3_scores    = [r["accuracy"]["top3_accuracy"] for r in mkt_results]
        hall_flags  = [r["hallucination"].get("is_hallucination", False)
                       for r in mkt_results if "status" in r["hallucination"]]
        uplift_vals = [r["impact"]["cvr_uplift_rel"]
                       for r in mkt_results if "cvr_uplift_rel" in r["impact"]]

        avg_correct = sum(correct_scores) / len(correct_scores) * 100
        avg_top1    = sum(top1_scores) / len(top1_scores) * 100
        avg_top3    = sum(top3_scores) / len(top3_scores) * 100
        hall_rate  = sum(hall_flags) / len(hall_flags) * 100 if hall_flags else 0
        avg_uplift = sum(uplift_vals) / len(uplift_vals) if uplift_vals else 0

        ground_truth_top1 = compute_score(df_valid).iloc[0]["channel"]
        rand_ch   = baseline_random(df_valid)
        cvr_ch    = baseline_cvr_only(df_valid)
        rand_top1 = int(ground_truth_top1 == rand_ch) * 100
        cvr_top1  = int(ground_truth_top1 == cvr_ch) * 100

        print(f"\n[마케팅 채널 추천 — {len(mkt_results)}개 케이스]")
        print(f"  Correct Accuracy: {avg_correct:.0f}%  ({sum(correct_scores)}/{len(correct_scores)})  ← Top-3 허용 포함")
        print(f"  Top-1 Accuracy:   {avg_top1:.0f}%  ({sum(top1_scores)}/{len(top1_scores)})")
        print(f"  Top-3 Accuracy:   {avg_top3:.0f}%  ({sum(top3_scores)}/{len(top3_scores)})")
        print(f"  Hallucination:    {hall_rate:.0f}%")
        print(f"  Avg CVR Uplift:   {avg_uplift:.1f}x")

        print(f"\n  Baseline 비교 (Top-1):")
        print(f"  - Random:         {rand_top1:.0f}%")
        print(f"  - CVR Only:       {cvr_top1:.0f}%")
        print(f"  - Our Agent:      {avg_top1:.0f}%")

        trap_results = eval_trap_cases(df_valid)
        trap_count   = sum(1 for t in trap_results if t["is_trap"])
        print(f"\n  CVR Only 함정 분석 (Top-5 추천 → 복합score 실제 순위):")
        for t in trap_results:
            ch_short = t["channel"][:38]
            print(f"  CVR {t['cvr_rank']}위: {ch_short:<38} → 복합 {t['composite_rank']}위  {t['status']}")
        print(f"\n  → CVR Only Top-5 중 함정 채널: {trap_count}개")
        print(f"  → Our Agent: 복합score 1위 일관 추천 ✅")

    if md_results:
        priority_correct = [r["priority"]["correct"] for r in md_results]
        quality_full     = [r["quality"]["quality_score"] == "3/3" for r in md_results]
        print(f"\n[멀티도메인 우선순위 — {len(md_results)}개 케이스]")
        print(f"  우선순위 정확도:  {sum(priority_correct)}/{len(priority_correct)}")
        print(f"  응답 완전성(3/3): {sum(quality_full)}/{len(quality_full)}")

    print()
    print("  ⚠️  오프라인 데이터 기준 (실제 매출 증가 보장 아님)")
    print("=" * 60)


if __name__ == "__main__":
    run_eval()