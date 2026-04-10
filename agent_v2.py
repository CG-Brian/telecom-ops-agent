"""
마케팅 / 영업 / CS 통합 의사결정 에이전트 v2
- 의사결정 유형 분류
- 멀티 도메인 SQL 병렬 실행
- 직접/간접 원인 구분
- Conflict Detection
- Evidence Aggregation
- Fallback Reasoning
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from typing import Any, Optional

import requests

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from snowflake.snowpark import Session

# -----------------------------------------------------------------------------
# 설정
# -----------------------------------------------------------------------------
DEFAULT_ACCOUNT = "SQHVTHB-UX70775"
DEFAULT_USER = "CGBrian"
DEFAULT_WAREHOUSE = "COMPUTE_WH"
DEFAULT_DATABASE = "HACKATHON_DB"
DEFAULT_SCHEMA = "ANALYTICS"
DEFAULT_SEMANTIC_VIEW = "HACKATHON_DB.ANALYTICS.KR_TELECOM_CONTRACTS_MARKETING_CALL_INSIGHTS"
CORTEX_MODEL = "mistral-large2"

_SESSION: Optional[Session] = None
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


# -----------------------------------------------------------------------------
# 세션
# -----------------------------------------------------------------------------
def _require_env_password() -> str:
    pwd = os.environ.get("SNOWFLAKE_PASSWORD", "").strip()
    if not pwd:
        raise RuntimeError("SNOWFLAKE_PASSWORD 환경변수가 없습니다.")
    return pwd


def get_session() -> Session:
    global _SESSION
    if load_dotenv:
        load_dotenv()
    if _SESSION is not None:
        try:
            if not _SESSION._conn.is_closed():
                return _SESSION
        except Exception:
            pass
        try:
            _SESSION.close()
        except Exception:
            pass
        _SESSION = None
    params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", DEFAULT_ACCOUNT),
        "user": os.getenv("SNOWFLAKE_USER", DEFAULT_USER),
        "password": _require_env_password(),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE),
        "database": os.getenv("SNOWFLAKE_DATABASE", DEFAULT_DATABASE),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", DEFAULT_SCHEMA),
    }
    _SESSION = Session.builder.configs(params).create()
    logger.info("Snowpark 세션 생성 완료")
    return _SESSION


def semantic_view_fqn() -> str:
    return os.getenv("SNOWFLAKE_SEMANTIC_VIEW", DEFAULT_SEMANTIC_VIEW)


# -----------------------------------------------------------------------------
# Step 1 — 의사결정 유형 + 도메인 파싱
# -----------------------------------------------------------------------------
DECISION_TYPES = {
    "root_cause":    ["왜", "이유", "원인", "낮은", "문제", "안되"],
    "priority":      ["뭐부터", "우선순위", "먼저", "순서", "중요한"],
    "budget_action": ["예산", "돈", "투자", "집중", "써야", "줄이"],
    "ops_action":    ["인력", "부족", "대응", "운영", "배치", "시간대"],
    "regional":      ["강남", "송파", "마포", "지역", "구", "동"],
}

def parse_intent(user_question: str) -> dict[str, Any]:
    q = user_question.lower()

    # 도메인 파악
    domains: set[str] = set()
    if any(k in q for k in ("마케팅", "채널", "cvr", "utm", "광고", "예산", "키워드", "네이버", "카카오", "유입")):
        domains.add("marketing")
    if any(k in q for k in ("퍼널", "전환", "병목", "이탈", "개통", "접수", "계약", "상담")):
        domains.add("funnel")
        domains.add("marketing")
    if any(k in q for k in ("cs", "콜", "연결", "인력", "상담원", "통화", "수신", "발신")):
        domains.add("cs")
    if any(k in q for k in ("강남", "송파", "지역", "서울", "계약", "매출", "영업")):
        domains.add("sales")
    if not domains:
        domains = {"marketing", "funnel", "cs"}

    # 의사결정 유형
    decision_type = "general"
    for dtype, keywords in DECISION_TYPES.items():
        if any(k in q for k in keywords):
            decision_type = dtype
            break

    # 지역 추출
    region = None
    for r in ["강남구", "송파구", "마포구", "서초구", "관악구", "노원구"]:
        if r in user_question:
            region = r
            break

    analyst_parts = [user_question.strip()]
    if "marketing" in domains:
        analyst_parts.append("마케팅 채널(utm_source, utm_medium)별 세션, 계약 수, 계약 전환율, 매출을 요약해 주세요.")
    if "funnel" in domains:
        analyst_parts.append("상품 카테고리별 퍼널 단계(상담요청→접수→개통→지급) 전환율과 단계별 이탈폭을 분석해 주세요.")
    if "cs" in domains:
        analyst_parts.append("콜센터 요일·시간대별 통화 건수와 연결률을 분석해 주세요.")
    if "sales" in domains:
        analyst_parts.append("지역별 계약 건수와 매출 트렌드를 분석해 주세요.")

    return {
        "domains": sorted(domains),
        "decision_type": decision_type,
        "region": region,
        "analyst_query": " ".join(analyst_parts),
        "raw_question": user_question,
    }


# -----------------------------------------------------------------------------
# REST 인증
# ★ 수정: 토큰 가져오는 방식 안정화 — 여러 경로 시도
# -----------------------------------------------------------------------------
def _get_rest_auth(session: Session) -> tuple[str, Optional[str]]:
    # 1순위: .env에 PAT 있으면 그걸 씀 (가장 안정적)
    pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
    if pat:
        return pat, "PROGRAMMATIC_ACCESS_TOKEN"

    # 2순위: 세션 내부 토큰 (여러 경로 시도)
    conn = session._conn._conn
    tok = None

    # 경로 1
    try:
        tok = conn._rest.token
    except Exception:
        pass

    # 경로 2
    if not tok:
        try:
            tok = conn.rest.token
        except Exception:
            pass

    # 경로 3
    if not tok:
        try:
            tok = session._conn._conn._token
        except Exception:
            pass

    if not tok:
        raise RuntimeError(
            "REST 호출용 토큰이 없습니다. "
            ".env에 SNOWFLAKE_PAT=<토큰값> 을 추가하세요. "
            "Snowsight → 프로필 → Settings → Authentication → Generate new token"
        )
    return tok, None


def _analyst_api_url(session: Session) -> str:
    override = os.environ.get("SNOWFLAKE_HOST", "").strip()
    if override:
        base = override.rstrip("/")
        if not base.lower().startswith("http"):
            base = f"https://{base}"
    else:
        conn = session._conn._conn
        base = f"https://{conn.host}"
    return f"{base}/api/v2/cortex/analyst/message"


# -----------------------------------------------------------------------------
# Step 2 — Cortex Analyst 호출
# ★ 수정: timeout 60초로 증가 + 1회 재시도
# -----------------------------------------------------------------------------
def call_cortex_analyst(
    session: Session,
    user_text: str,
    timeout_sec: int = 60,   # 30 → 60
    retry: int = 1,
) -> dict[str, Any]:
    token, token_type = _get_rest_auth(session)
    url = _analyst_api_url(session)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if token_type:
        headers["X-Snowflake-Authorization-Token-Type"] = token_type

    body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": user_text}]}],
        "semantic_view": semantic_view_fqn(),
        "database": "HACKATHON_DB",
        "schema": "ANALYTICS",
    }

    last_err = None
    for attempt in range(retry + 1):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=timeout_sec)
            try:
                payload = resp.json()
            except Exception:
                payload = {"_raw_text": resp.text}
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Cortex Analyst HTTP {resp.status_code}: "
                    f"{json.dumps(payload, ensure_ascii=False)[:500]}"
                )
            return payload
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            if attempt < retry:
                logger.warning(f"Cortex Analyst 연결 실패 ({attempt+1}/{retry+1}), 3초 후 재시도...")
                time.sleep(3)
            continue
        except RuntimeError:
            raise

    raise RuntimeError(f"Cortex Analyst 호출 실패 (재시도 {retry}회): {last_err}")


def extract_sql(payload: dict[str, Any]) -> tuple[Optional[str], str]:
    msg = payload.get("message") or {}
    contents = msg.get("content") or []
    sql_stmt = None
    texts = []
    for block in contents:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "sql" and block.get("statement"):
            sql_stmt = str(block["statement"]).strip()
        elif block.get("type") == "text" and block.get("text"):
            texts.append(str(block["text"]))
    return sql_stmt, "\n".join(texts)


def run_sql(session: Session, sql: Optional[str]) -> list[dict[str, Any]]:
    if not sql:
        return []
    try:
        rows = session.sql(sql).collect()
    except Exception as e:
        raise RuntimeError(f"SQL 실행 실패: {e}") from e
    out = []
    for r in rows:
        if hasattr(r, "as_dict"):
            out.append(r.as_dict())
        elif hasattr(r, "_mapping"):
            out.append(dict(r._mapping))
        else:
            out.append(dict(r))
    return out


# -----------------------------------------------------------------------------
# Step 3 — 도메인별 Evidence 추출
# -----------------------------------------------------------------------------
def extract_marketing_evidence(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"available": False}

    best_ch = None
    best_score = -1.0
    best_cvr = 0.0
    best_revenue = 0.0
    best_sessions = 0.0
    all_cvrs = []

    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        cvr = None
        for key in ("CONTRACT_CVR", "WEIGHTED_CONTRACT_CVR"):
            if key in u and u[key] is not None:
                try:
                    cvr = float(u[key])
                    break
                except (TypeError, ValueError):
                    pass

        sessions = 0.0
        try:
            sessions = float(u.get("TOTAL_SESSIONS") or 0)
        except (TypeError, ValueError):
            pass

        contracts = 0.0
        try:
            contracts = float(u.get("TOTAL_CONTRACTS") or 0)
        except (TypeError, ValueError):
            pass

        revenue = 0.0
        try:
            revenue = float(u.get("TOTAL_REVENUE") or 0)
        except (TypeError, ValueError):
            pass

        src = u.get("UTM_SOURCE") or ""
        med = u.get("UTM_MEDIUM") or ""
        channel = f"{src}/{med}" if (src and med) else src

        if not (cvr is not None and channel and sessions >= 500 and contracts >= 50):
            continue

        all_cvrs.append(cvr)
        rev_per_session = revenue / sessions if sessions > 0 else 0
        score = cvr * 0.4 + (rev_per_session / 100_000) * 0.6

        if score > best_score:
            best_score = score
            best_cvr = cvr
            best_ch = channel
            best_revenue = revenue
            best_sessions = sessions

    if not best_ch:
        return {"available": False, "note": "유효 채널 없음 (세션/계약 기준 미달)"}

    avg_cvr = sum(all_cvrs) / len(all_cvrs) if all_cvrs else 0
    cvr_vs_avg = round((best_cvr - avg_cvr) / avg_cvr * 100, 1) if avg_cvr > 0 else 0
    rank = sorted(all_cvrs, reverse=True).index(best_cvr) + 1
    signal = "good" if cvr_vs_avg > 100 else "neutral" if cvr_vs_avg > 0 else "bad"

    return {
        "available": True,
        "best_channel": best_ch,
        "cvr": round(best_cvr * 100, 1),
        "cvr_vs_avg_pct": cvr_vs_avg,
        "rank": rank,
        "total_channels": len(all_cvrs),
        "total_revenue": best_revenue,
        "total_sessions": best_sessions,
        "signal": signal,
        "summary": f"{best_ch} 채널이 전체 평균 대비 +{cvr_vs_avg}%, {len(all_cvrs)}개 채널 중 {rank}위",
    }


def extract_funnel_evidence(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"available": False}

    funnel_keys = (
        "CVR_CONSULT_REQUEST", "CVR_REGISTEND", "CVR_OPEN", "CVR_PAYEND",
        "AVG_CVR_CONSULT_REQUEST", "AVG_CVR_REGISTEND", "AVG_CVR_OPEN", "AVG_CVR_PAYEND",
    )
    agg: dict[str, list[float]] = {k: [] for k in funnel_keys}
    drop_keys = (
        "DROP_CONSULT_TO_REGIST", "DROP_REGIST_TO_OPEN", "DROP_OPEN_TO_PAYEND",
        "DROPOFF_CONSULT_TO_REGIST", "DROPOFF_REGIST_TO_OPEN", "DROPOFF_OPEN_TO_PAYEND",
    )
    drop_agg: dict[str, list[float]] = {k: [] for k in drop_keys}

    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        for k in funnel_keys:
            if k in u and u[k] is not None:
                try:
                    agg[k].append(float(u[k]))
                except (TypeError, ValueError):
                    pass
        for k in drop_keys:
            if k in u and u[k] is not None:
                try:
                    drop_agg[k].append(float(u[k]))
                except (TypeError, ValueError):
                    pass

    means = {k: (sum(v) / len(v) if v else None) for k, v in agg.items()}
    valid = {k: v for k, v in means.items() if v is not None}
    drop_means = {k: (sum(v) / len(v) if v else None) for k, v in drop_agg.items()}
    valid_drops = {k: v for k, v in drop_means.items() if v is not None}

    if valid:
        stage = min(valid, key=valid.get)
        label_map = {
            "CVR_CONSULT_REQUEST":     "상담요청",
            "CVR_REGISTEND":           "접수",
            "CVR_OPEN":                "개통",
            "CVR_PAYEND":              "지급",
            "AVG_CVR_CONSULT_REQUEST": "상담요청",
            "AVG_CVR_REGISTEND":       "접수",
            "AVG_CVR_OPEN":            "개통",
            "AVG_CVR_PAYEND":          "지급",
        }
        bottleneck = label_map.get(stage, stage)
        bottleneck_cvr = round(valid[stage], 1)

        drop_map = {
            "DROP_CONSULT_TO_REGIST": "상담→접수",
            "DROP_REGIST_TO_OPEN":    "접수→개통",
            "DROP_OPEN_TO_PAYEND":    "개통→지급",
        }
        max_drop = None
        max_drop_val = -1.0
        for dk, dl in drop_map.items():
            if dk in valid_drops and valid_drops[dk] > max_drop_val:
                max_drop_val = valid_drops[dk]
                max_drop = dl

        signal = "bad" if bottleneck_cvr < 30 else "neutral" if bottleneck_cvr < 60 else "good"

        return {
            "available": True,
            "bottleneck_stage": bottleneck,
            "bottleneck_cvr": bottleneck_cvr,
            "max_drop_stage": max_drop,
            "max_drop_value": round(max_drop_val, 1) if max_drop else None,
            "all_stage_cvrs": {label_map.get(k, k): round(v, 1) for k, v in valid.items()},
            "signal": signal,
            "summary": f"{bottleneck} 단계 CVR {bottleneck_cvr}%로 병목 발생"
                       + (f", 최대 이탈폭: {max_drop} {max_drop_val:.1f}%p" if max_drop else ""),
        }

    return {"available": False, "note": "퍼널 CVR 컬럼 없음"}


def extract_cs_evidence(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"available": False}

    conn_rates = []
    peak_day = ""
    peak_hour = ""

    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        if u.get("DAY_OF_WEEK_NAME"):
            peak_day = str(u["DAY_OF_WEEK_NAME"])
        if u.get("HOUR_OF_DAY") is not None:
            peak_hour = str(u["HOUR_OF_DAY"])
        for key in ("WEIGHTED_CONNECTION_RATE", "CONNECTION_RATE", "AVG_CONNECTION_RATE"):
            if key in u and u[key] is not None:
                try:
                    v = float(u[key])
                    if v > 1:
                        v = v / 100
                    conn_rates.append(v)
                    break
                except (TypeError, ValueError):
                    pass

    if not conn_rates:
        return {"available": False, "note": "연결률 데이터 없음"}

    avg_cr = sum(conn_rates) / len(conn_rates)
    below_target = avg_cr < 0.70
    signal = "bad" if below_target else "good"
    peak = f"{peak_day} {peak_hour}시" if peak_day and peak_hour else "N/A"

    return {
        "available": True,
        "connection_rate": round(avg_cr * 100, 1),
        "target": 70.0,
        "below_target": below_target,
        "gap_to_target": round((0.70 - avg_cr) * 100, 1) if below_target else 0,
        "peak_time": peak,
        "signal": signal,
        "summary": f"연결률 {round(avg_cr*100,1)}%"
                   + (f" (목표 70% 대비 {round((0.70-avg_cr)*100,1)}%p 미달)" if below_target else " (목표 달성)"),
    }


def extract_sales_evidence(rows: list[dict[str, Any]], region: Optional[str] = None) -> dict[str, Any]:
    if not rows:
        return {"available": False}

    total_contracts = 0
    total_revenue = 0.0
    region_found = False

    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        try:
            total_contracts += int(u.get("CONTRACT_COUNT") or u.get("CONTRACTS") or 0)
        except (TypeError, ValueError):
            pass
        try:
            total_revenue += float(u.get("TOTAL_NET_SALES") or u.get("REVENUE") or 0)
        except (TypeError, ValueError):
            pass
        if region and str(u.get("INSTALL_CITY") or "").strip() == region:
            region_found = True

    if total_contracts == 0:
        return {"available": False}

    return {
        "available": True,
        "total_contracts": total_contracts,
        "total_revenue": total_revenue,
        "region": region,
        "region_found": region_found,
        "summary": f"총 계약 {total_contracts:,}건, 매출 {int(total_revenue):,}원"
                   + (f" ({region} 데이터 포함)" if region_found else ""),
    }


# -----------------------------------------------------------------------------
# Step 4 — Conflict Detection
# -----------------------------------------------------------------------------
def detect_conflicts(evidence: dict[str, Any]) -> dict[str, Any]:
    signals = {}
    for domain, ev in evidence.items():
        if isinstance(ev, dict) and ev.get("available"):
            signals[domain] = ev.get("signal", "neutral")

    conflicts = []
    interpretation = ""

    mkt_sig    = signals.get("marketing")
    funnel_sig = signals.get("funnel")
    cs_sig     = signals.get("cs")

    if mkt_sig == "good" and funnel_sig == "bad":
        conflicts.append("마케팅 유입은 양호하나 후속 전환에서 손실 발생")
        interpretation = "유입 자체는 양호하나, 후속 전환 프로세스에서 성과가 깎이고 있습니다."
    elif mkt_sig == "bad" and funnel_sig == "good":
        conflicts.append("내부 전환 구조는 양호하나 유입 채널 비효율")
        interpretation = "내부 전환 구조는 양호하나 유입 품질/채널 배분이 비효율적입니다."
    elif mkt_sig == "good" and cs_sig == "bad":
        conflicts.append("마케팅 성과는 좋으나 CS 연결 이슈로 고객 이탈 위험")
        interpretation = "마케팅 유입은 좋지만, CS 연결 문제로 최종 계약까지 연결되지 않을 위험이 있습니다."
    elif all(s == "good" for s in signals.values() if s):
        interpretation = "전체 지표가 양호한 상태입니다. 현 전략을 유지하며 최적화에 집중하세요."
    elif all(s == "bad" for s in signals.values() if s):
        interpretation = "전 영역에 걸쳐 개선이 필요합니다. 퍼널 병목 해결을 최우선으로 하세요."

    return {"signals": signals, "conflicts": conflicts, "interpretation": interpretation}


# -----------------------------------------------------------------------------
# Step 5 — 우선순위화
# -----------------------------------------------------------------------------
def prioritize_actions(evidence: dict[str, Any], decision_type: str) -> list[dict[str, Any]]:
    priorities = []

    funnel_ev = evidence.get("funnel", {})
    mkt_ev    = evidence.get("marketing", {})
    cs_ev     = evidence.get("cs", {})

    if funnel_ev.get("available") and funnel_ev.get("signal") == "bad":
        priorities.append({
            "rank": 1,
            "area": "퍼널",
            "action": f"{funnel_ev.get('bottleneck_stage', '병목')} 단계 개선",
            "impact": "높음", "urgency": "높음", "difficulty": "중간",
        })

    if mkt_ev.get("available") and mkt_ev.get("cvr_vs_avg_pct", 0) > 100:
        priorities.append({
            "rank": 2,
            "area": "마케팅",
            "action": f"{mkt_ev.get('best_channel')} 예산 20~30% 확대 A/B 테스트",
            "impact": "높음", "urgency": "중간", "difficulty": "낮음",
        })

    if cs_ev.get("available"):
        if cs_ev.get("below_target"):
            priorities.append({
                "rank": 3,
                "area": "CS",
                "action": f"{cs_ev.get('peak_time')} 시간대 인력 배치 최적화",
                "impact": "중간", "urgency": "높음", "difficulty": "낮음",
            })
        else:
            priorities.append({
                "rank": len(priorities) + 1,
                "area": "CS",
                "action": f"{cs_ev.get('peak_time')} 시간대 선제적 모니터링",
                "impact": "낮음", "urgency": "낮음", "difficulty": "낮음",
            })

    for i, p in enumerate(priorities, 1):
        p["rank"] = i

    return priorities


# -----------------------------------------------------------------------------
# Step 6 — LLM 프롬프트 + Complete 호출
# ★ 수정: Complete → complete (소문자), SQL 폴백 유지
# -----------------------------------------------------------------------------
EMPTY_OUTPUT = {
    "conclusion": "",
    "direct_cause": "",
    "indirect_cause": "",
    "action_items": [],
    "confidence": "",
    "reasoning": "",
}


def build_prompt_v2(
    user_question: str,
    intent: dict[str, Any],
    evidence: dict[str, Any],
    conflicts: dict[str, Any],
    priorities: list[dict[str, Any]],
    sql_results_summary: str,
) -> str:
    evidence_json  = json.dumps(evidence,   ensure_ascii=False, default=str, indent=2)
    conflicts_json = json.dumps(conflicts,  ensure_ascii=False, default=str)
    priorities_json = json.dumps(priorities, ensure_ascii=False, default=str)

    decision_hints = {
        "root_cause":    "직접 원인과 간접 원인을 구분해서 설명하세요.",
        "priority":      "우선순위를 1~3순위로 명확히 제시하세요.",
        "budget_action": "예산 배분 관점에서 구체적인 수치와 함께 설명하세요.",
        "ops_action":    "운영 관점에서 즉시 실행 가능한 액션을 제시하세요.",
        "regional":      "지역 특성을 고려한 맞춤형 전략을 제시하세요.",
        "general":       "전반적인 상황을 분석하고 핵심 인사이트를 도출하세요.",
    }
    decision_hint = decision_hints.get(intent.get("decision_type", "general"), "")

    return f"""당신은 통신사 마케팅·영업·CS 통합 운영 어드바이저입니다.
아래 도메인별 분석 근거를 바탕으로 통합적인 의사결정을 내려주세요.

[의사결정 유형]
{intent.get('decision_type', 'general')} - {decision_hint}

[도메인별 Evidence 요약]
{evidence_json}

[Conflict Detection]
{conflicts_json}

[우선순위 분석]
{priorities_json}

[SQL 결과 요약]
{sql_results_summary}

[규칙]
1) 숫자는 Evidence에 있는 값만 사용. 없으면 "데이터 제한적"으로 표현.
2) 데이터가 없는 도메인은 fallback_reasoning으로 처리.
3) 출력은 JSON만.
4) action_items 첫 번째는 "⭐ [액션] (우선 실행)" 형태.
5) confidence는 "높음/보통/낮음" 중 하나.
6) direct_cause: 데이터로 직접 확인된 원인
   indirect_cause: 추론된 간접 원인 또는 가설

출력 JSON 스키마:
{json.dumps(EMPTY_OUTPUT, ensure_ascii=False, indent=2)}

[사용자 질문]
{user_question}

위 근거만 사용해서 JSON을 작성하세요.
"""


def parse_json(text: str) -> dict[str, Any]:
    text = text.strip()
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*',     '', text)
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
    return {**EMPTY_OUTPUT, "reasoning": "파싱 실패: " + text[:300]}


def cortex_complete(session: Session, prompt: str) -> str:
    # 1순위: snowflake-ml-python의 complete() (소문자)
    try:
        from snowflake.cortex import complete  # ★ 소문자로 수정
        out = complete(CORTEX_MODEL, prompt, session=session)
        return out if isinstance(out, str) else str(out)
    except ImportError:
        logger.info("snowflake.cortex.complete 미설치 — SQL 폴백")
    except Exception as e:
        logger.warning("complete() 실패, SQL 폴백: %s", e)

    # 2순위: SQL 방식 (항상 작동)
    safe_prompt = prompt.replace("'", "''")  # SQL 인젝션 방지
    rows = session.sql(
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_MODEL}', $$ {safe_prompt} $$)"
    ).collect()
    if not rows:
        return ""
    return str(rows[0][0])


# -----------------------------------------------------------------------------
# 통합 실행
# -----------------------------------------------------------------------------
def run_agent(user_question: str) -> dict[str, Any]:
    session = get_session()
    intent  = parse_intent(user_question)

    logger.info(f"도메인: {intent['domains']}, 유형: {intent['decision_type']}")

    DOMAIN_QUERIES = {
        "marketing": "마케팅 채널(utm_source, utm_medium)별 세션, 계약 수, 계약 전환율, 매출을 요약해 주세요.",
        "funnel":    "상품 카테고리별 퍼널 단계 전환율과 단계별 이탈폭을 분석해 주세요.",
        "cs":        "콜센터 요일·시간대별 통화 건수와 연결률을 분석해 주세요.",
        "sales":     "지역별 계약 건수와 매출 트렌드를 분석해 주세요.",
    }

    evidence:  dict[str, Any] = {}
    sql_stmts: dict[str, str] = {}
    analyst_note = ""

    for domain in intent["domains"]:
        query = DOMAIN_QUERIES.get(domain)
        if not query:
            continue
        try:
            payload  = call_cortex_analyst(session, query)
            sql_stmt, _ = extract_sql(payload)
            if sql_stmt:
                sql_stmts[domain] = sql_stmt
                rows = run_sql(session, sql_stmt)
                if domain == "marketing":
                    evidence["marketing"] = extract_marketing_evidence(rows)
                elif domain == "funnel":
                    evidence["funnel"]    = extract_funnel_evidence(rows)
                elif domain == "cs":
                    evidence["cs"]        = extract_cs_evidence(rows)
                elif domain == "sales":
                    evidence["sales"]     = extract_sales_evidence(rows, intent.get("region"))
            else:
                evidence[domain] = {"available": False, "note": "SQL 생성 실패"}
        except Exception as e:
            logger.warning(f"{domain} 분석 실패: {e}")
            evidence[domain] = {"available": False, "note": str(e)}

    conflicts  = detect_conflicts(evidence)
    priorities = prioritize_actions(evidence, intent["decision_type"])

    available_summaries = [
        ev.get("summary", "")
        for ev in evidence.values()
        if isinstance(ev, dict) and ev.get("available")
    ]
    sql_results_summary = (
        "\n".join(f"- {s}" for s in available_summaries)
        if available_summaries else "데이터 없음"
    )

    prompt = build_prompt_v2(
        user_question, intent, evidence, conflicts, priorities, sql_results_summary
    )
    try:
        llm_raw = cortex_complete(session, prompt)
    except Exception as e:
        logger.exception("Cortex Complete 실패")
        llm_raw = json.dumps(
            {**EMPTY_OUTPUT, "reasoning": f"Complete 실패: {e}"}, ensure_ascii=False
        )

    output = parse_json(llm_raw)

    return {
        **output,
        "_meta": {
            "intent":       intent,
            "analyst_sql":  sql_stmts,
            "row_count":    sum(1 for v in evidence.values() if isinstance(v, dict) and v.get("available")),
            "evidence":     evidence,
            "conflicts":    conflicts,
            "priorities":   priorities,
            "analyst_note": analyst_note,
        },
    }


# -----------------------------------------------------------------------------
# 데모
# -----------------------------------------------------------------------------
DEMO_QUESTIONS = [
    "전환율이 낮은 이유가 뭐야?",
    "지금 성과를 올리려면 뭐부터 해야 해?",
    "어떤 마케팅 채널이 제일 효율적이야?",
]

if __name__ == "__main__":
    if load_dotenv:
        load_dotenv()
    print("=== 통합 에이전트 v2 데모 ===\n")
    for i, q in enumerate(DEMO_QUESTIONS, 1):
        print(f"--- 질문 {i} ---\n{q}\n")
        try:
            out = run_agent(q)
            print(json.dumps(out, ensure_ascii=False, indent=2))
        except Exception as exc:
            print(f"오류: {exc}", file=sys.stderr)
        print("\n")