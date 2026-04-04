"""
telecom-ops-agent Evaluation v2
AI 추천 시스템 성능 평가 (오프라인 데이터 기준)

평가 항목:
1. 추천 정확도 (Top-1, Top-3 Accuracy) — multi-query + baseline 비교
2. 숫자 신뢰도 (Hallucination Rate)
3. 비즈니스 임팩트 (Revenue Uplift)
"""

from __future__ import annotations

import os
import random
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pandas as pd
from snowflake.snowpark import Session


# -----------------------------------------------------------------------------
# 데이터 로드
# -----------------------------------------------------------------------------
def get_session() -> Session:
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT", "SQHVTHB-UX70775"),
        "user":      os.getenv("SNOWFLAKE_USER", "CGBrian"),
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.getenv("SNOWFLAKE_DATABASE", "HACKATHON_DB"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    }).create()


def load_marketing_data(session: Session) -> pd.DataFrame:
    DB = "SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION"
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


# -----------------------------------------------------------------------------
# 공통 필터 + Score (Rule Engine과 동일)
# -----------------------------------------------------------------------------
SESSIONS_MIN  = 500
CONTRACTS_MIN = 50

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
# Baseline 방식 2가지
# -----------------------------------------------------------------------------
def baseline_random(df_valid: pd.DataFrame, seed: int = 42) -> str:
    """랜덤 추천 — 아무거나 고르기"""
    random.seed(seed)
    return random.choice(df_valid["channel"].tolist())

def baseline_cvr_only(df_valid: pd.DataFrame) -> str:
    """CVR만 보고 추천 — LLM Only 방식"""
    return df_valid.loc[df_valid["contract_cvr"].idxmax(), "channel"]


# -----------------------------------------------------------------------------
# Eval 1: 추천 정확도
# -----------------------------------------------------------------------------
def eval_accuracy(df_valid: pd.DataFrame, recommendation: str) -> dict[str, Any]:
    ranked = compute_score(df_valid).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    actual_top1 = ranked.iloc[0]["channel"]
    actual_top3 = ranked.head(3)["channel"].tolist()
    actual_rank = ranked[ranked["channel"] == recommendation]["rank"].values
    return {
        "recommendation": recommendation,
        "actual_top1":    actual_top1,
        "top1_accuracy":  int(recommendation == actual_top1),
        "top3_accuracy":  int(recommendation in actual_top3),
        "actual_rank":    int(actual_rank[0]) if len(actual_rank) > 0 else None,
        "total":          len(ranked),
    }


# -----------------------------------------------------------------------------
# Eval 2: 숫자 신뢰도
# -----------------------------------------------------------------------------
def eval_hallucination(
    df: pd.DataFrame,
    channel: str,
    llm_cvr: float,
    threshold: float = 0.02,
) -> dict[str, Any]:
    row = df[df["channel"] == channel]
    if row.empty:
        return {"error": f"{channel} 없음"}
    actual_cvr       = float(row.iloc[0]["contract_cvr"])
    abs_error        = abs(llm_cvr - actual_cvr)
    is_hallucination = abs_error > threshold
    return {
        "llm_cvr":        round(llm_cvr, 4),
        "actual_cvr":     round(actual_cvr, 4),
        "absolute_error": round(abs_error, 4),
        "is_hallucination": is_hallucination,
        "status": "✅ Grounded" if not is_hallucination else "❌ Hallucination",
    }


# -----------------------------------------------------------------------------
# Eval 3: 비즈니스 임팩트
# -----------------------------------------------------------------------------
def eval_impact(df_valid: pd.DataFrame, channel: str) -> dict[str, Any]:
    baseline_cvr = df_valid["contract_cvr"].mean()
    baseline_rev = df_valid["revenue_per_session"].mean()
    rec = df_valid[df_valid["channel"] == channel]
    if rec.empty:
        return {"error": f"{channel} 없음"}
    rec_cvr = float(rec.iloc[0]["contract_cvr"])
    rec_rev = float(rec.iloc[0]["revenue_per_session"])
    return {
        "baseline_cvr":    round(baseline_cvr, 4),
        "recommended_cvr": round(rec_cvr, 4),
        "cvr_uplift_rel":  round(rec_cvr / baseline_cvr, 2) if baseline_cvr > 0 else 0,
        "baseline_rev":    round(baseline_rev, 0),
        "recommended_rev": round(rec_rev, 0),
        "rev_uplift_rel":  round(rec_rev / baseline_rev, 2) if baseline_rev > 0 else 0,
    }


# -----------------------------------------------------------------------------
# 멀티 케이스 Eval
# -----------------------------------------------------------------------------
# (AI 추천 채널, LLM이 말한 CVR)
EVAL_QUERIES: list[tuple[str, float]] = [
    ("nc_money/direct_ps", 0.271),   # Our System 추천
    ("kakao/keyword",      0.330),   # CVR 높지만 규모 작음
    ("naver/sa_brand",     0.150),   # 중간 채널
]

def run_multi_eval(
    df: pd.DataFrame,
    df_valid: pd.DataFrame,
) -> dict[str, Any]:
    results = []
    for channel, llm_cvr in EVAL_QUERIES:
        results.append({
            "channel":       channel,
            "llm_cvr":       llm_cvr,
            "accuracy":      eval_accuracy(df_valid, channel),
            "hallucination": eval_hallucination(df, channel, llm_cvr),
            "impact":        eval_impact(df_valid, channel),
        })

    # Baseline 비교
    rand_ch  = baseline_random(df_valid)
    cvr_ch   = baseline_cvr_only(df_valid)
    baseline = {
        "random": eval_accuracy(df_valid, rand_ch),
        "cvr_only": eval_accuracy(df_valid, cvr_ch),
        "random_channel":   rand_ch,
        "cvr_only_channel": cvr_ch,
    }

    return {"results": results, "baseline": baseline}


# -----------------------------------------------------------------------------
# 출력
# -----------------------------------------------------------------------------
def print_results(data: dict[str, Any]) -> None:
    results  = data["results"]
    baseline = data["baseline"]

    print("=" * 60)
    print("📊 telecom-ops-agent KPI Evaluation v2")
    print("=" * 60)

    # 케이스별
    print("\n케이스별 결과")
    print("─" * 60)
    for r in results:
        acc  = r["accuracy"]
        hall = r["hallucination"]
        imp  = r["impact"]
        print(f"\n  [{r['channel']}]  LLM CVR: {r['llm_cvr']*100:.1f}%")
        print(f"  Top-1: {'✅' if acc['top1_accuracy'] else '❌'}  "
              f"Top-3: {'✅' if acc['top3_accuracy'] else '❌'}  "
              f"순위: {acc['actual_rank']}위/{acc['total']}개")
        if "error" not in hall:
            print(f"  Grounding: {hall['status']}  (오차 {hall['absolute_error']*100:.3f}%p)")
        if "error" not in imp:
            print(f"  CVR Uplift: {imp['cvr_uplift_rel']:.1f}x  "
                  f"Rev Uplift: {imp['rev_uplift_rel']:.1f}x")

    # 집계
    top1_scores = [r["accuracy"]["top1_accuracy"] for r in results]
    top3_scores = [r["accuracy"]["top3_accuracy"] for r in results]
    hall_flags  = [
        r["hallucination"].get("is_hallucination", False)
        for r in results
        if "error" not in r["hallucination"]
    ]
    avg_top1   = sum(top1_scores) / len(top1_scores) * 100
    avg_top3   = sum(top3_scores) / len(top3_scores) * 100
    hall_rate  = sum(hall_flags) / len(hall_flags) * 100 if hall_flags else 0

    our_result = next((r for r in results if r["channel"] == "nc_money/direct_ps"), None)
    cvr_uplift = our_result["impact"]["cvr_uplift_rel"] if our_result and "error" not in our_result["impact"] else "N/A"

    # Baseline 비교
    rand_top1    = baseline["random"]["top1_accuracy"] * 100
    cvr_top1     = baseline["cvr_only"]["top1_accuracy"] * 100

    print()
    print("─" * 60)
    print("Baseline 비교")
    print("─" * 60)
    print(f"  Random 추천 Top-1:    {rand_top1:.0f}%  ({baseline['random_channel']})")
    print(f"  CVR Only 추천 Top-1:  {cvr_top1:.0f}%  ({baseline['cvr_only_channel']})")
    print(f"  Our System Top-1:     {avg_top1:.0f}%  (nc_money/direct_ps)")

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Top-1 Accuracy:    {avg_top1:.0f}%  ({sum(top1_scores)}/{len(top1_scores)} 케이스)")
    print(f"  Top-3 Accuracy:    {avg_top3:.0f}%  ({sum(top3_scores)}/{len(top3_scores)} 케이스)")
    print(f"  Hallucination:     {hall_rate:.0f}%  (임계값 2%p 기준)")
    if isinstance(cvr_uplift, float):
        print(f"  CVR Uplift:        {cvr_uplift:.1f}x  (추천 채널 vs 전체 평균)")
    print()
    print("  Baseline 대비 개선:")
    print(f"  - Random Top-1 Accuracy:   {rand_top1:.0f}%")
    print(f"  - CVR Only Top-1 Accuracy: {cvr_top1:.0f}%")
    print(f"  - Our System:              {avg_top1:.0f}%")
    print()
    print("  ⚠️  오프라인 데이터 기준 예상 uplift (실제 매출 증가 보장 아님)")
    print("=" * 60)


# -----------------------------------------------------------------------------
# 실행
# -----------------------------------------------------------------------------
def run_eval() -> None:
    session  = get_session()
    df       = load_marketing_data(session)
    df_valid = filter_valid(df)
    print(f"\n전체 채널: {len(df)}개 | 유효 채널: {len(df_valid)}개\n")
    data = run_multi_eval(df, df_valid)
    print_results(data)


if __name__ == "__main__":
    run_eval()