"""
마케팅 / 영업 / CS 통합 의사결정 에이전트
Snowflake Cortex Analyst(REST) + Cortex Complete(mistral-large2) + Snowpark Session
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from typing import Any, Optional

import requests

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[misc, assignment]

from snowflake.snowpark import Session

# -----------------------------------------------------------------------------
# 설정 (환경변수 / 기본값)
# -----------------------------------------------------------------------------
DEFAULT_ACCOUNT = "SQHVTHB-UX70775"
DEFAULT_USER = "CGBrian"
DEFAULT_WAREHOUSE = "COMPUTE_WH"
DEFAULT_DATABASE = "HACKATHON_DB"
DEFAULT_SCHEMA = "ANALYTICS"
DEFAULT_SEMANTIC_VIEW = "HACKATHON_DB.ANALYTICS.KR_TELECOM_CONTRACTS_MARKETING_CALL_INSIGHTS"
CORTEX_MODEL = "mistral-large2"

# 에이전트가 참고하는 사전 인사이트 (LLM 프롬프트 + 규칙 보조)
STATIC_CONTEXT = """
[사전 핵심 인사이트 — 데이터로 재검증 가능한 가설]
1) 마케팅: 카카오 키워드 CVR 약 32.9% vs 애드네트워크 약 0.008% (약 4,000배 격차) — 실제 수치는 Analyst SQL 결과를 따름.
2) 퍼널: 상담 유입 CVR이 병목(렌탈 약 28.7%, 인터넷 약 22%) — SQL로 단계별 CVR을 확인할 것.
3) CS: 수신 연결률 평균 약 55.8%, 목표 70% 미달. 피크: 일요일, 오후 14~16시 — SQL 결과가 있으면 그 숫자를 우선.
"""

# 모듈 단일 Snowpark 세션 (재사용)
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
        raise RuntimeError(
            "SNOWFLAKE_PASSWORD 환경변수가 없습니다. .env에 설정하세요."
        )
    return pwd


def get_session() -> Session:
    """Snowpark Session 싱글톤. 매 호출마다 새 연결을 만들지 않음."""
    global _SESSION
    if load_dotenv:
        load_dotenv()

    if _SESSION is not None:
        try:
            if not _SESSION._conn.is_closed():  # noqa: SLF001
                return _SESSION
        except Exception:  # noqa: BLE001
            pass
        try:
            _SESSION.close()
        except Exception:  # noqa: BLE001
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
    logger.info("Snowpark 세션 생성 완료 (재사용 모드)")
    return _SESSION


def semantic_view_fqn() -> str:
    """Cortex Analyst 요청에 넣을 시맨틱 뷰 FQN (공식 필드명: semantic_view)."""
    return os.getenv("SNOWFLAKE_SEMANTIC_VIEW", DEFAULT_SEMANTIC_VIEW)


# -----------------------------------------------------------------------------
# Step 1 — Intent Parsing
# -----------------------------------------------------------------------------
def parse_intent(user_question: str) -> dict[str, Any]:
    """
    사용자 질문에서 도메인과 필요한 액션 유형을 휴리스틱으로 추출.
    반환: domains, action_hints, analyst_query (Analyst에 넘길 자연어)
    """
    q = user_question.lower()
    domains: set[str] = set()

    # 마케팅
    if any(
        k in user_question or k in q
        for k in (
            "마케팅",
            "채널",
            "cvr",
            "utm",
            "광고",
            "예산",
            "키워드",
            "네이버",
            "카카오",
            "구글",
            "ga",
            "유입",
        )
    ):
        domains.add("marketing")

    # 영업 / 지역 계약
    if any(
        k in user_question or k in q
        for k in (
            "영업",
            "계약",
            "강남",
            "구",
            "지역",
            "설치",
            "순매출",
            "매출",
        )
    ):
        domains.add("sales")

    # CS
    if any(
        k in user_question or k in q
        for k in (
            "cs",
            "콜",
            "콜센터",
            "연결",
            "인력",
            "상담원",
            "통화",
            "수신",
            "발신",
        )
    ):
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

    # Analyst용 질의: 원문 + 도메인 힌트 (영어 키워드 병기로 SQL 생성 안정화)
    analyst_parts = [user_question.strip()]
    if "marketing" in domains:
        analyst_parts.append(
            "마케팅 채널(utm_source, utm_medium)별 세션, 계약 수, 계약 전환율, 매출을 요약해 주세요."
        )
    if "sales" in domains:
        analyst_parts.append(
            "지역(시도/시군구) 및 상품 카테고리별 계약 건수·매출·전환율 관련 지표를 조회해 주세요."
        )
    if "cs" in domains:
        analyst_parts.append(
            "콜센터 연결률, 요일·시간대별 통화 및 연결률을 조회해 주세요."
        )
    analyst_query = " ".join(analyst_parts)

    return {
        "domains": sorted(domains),
        "action_hints": action_hints,
        "analyst_query": analyst_query,
        "raw_question": user_question,
    }


# -----------------------------------------------------------------------------
# REST 인증 토큰 (PAT 우선, 없으면 커넥터 세션 토큰)
# -----------------------------------------------------------------------------
def _get_rest_auth(session: Session) -> tuple[str, Optional[str]]:
    """
    (token, x_snowflake_auth_token_type).
    PAT이 있으면 PROGRAMMATIC_ACCESS_TOKEN, 없으면 세션 토큰 + 타입 None(자동 추론).
    """
    pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
    if pat:
        return pat, "PROGRAMMATIC_ACCESS_TOKEN"

    conn = session._conn._conn  # noqa: SLF001
    tok = conn._rest.token  # noqa: SLF001
    if not tok:
        raise RuntimeError(
            "REST 호출용 토큰이 없습니다. SNOWFLAKE_PAT를 설정하거나 세션을 다시 여세요."
        )
    return tok, None


def _analyst_api_url(session: Session) -> str:
    override = os.environ.get("SNOWFLAKE_HOST", "").strip()
    if override:
        base = override.rstrip("/")
        if not base.lower().startswith("http"):
            base = f"https://{base}"
    else:
        conn = session._conn._conn  # noqa: SLF001
        base = f"https://{conn.host}"
    return f"{base}/api/v2/cortex/analyst/message"


# -----------------------------------------------------------------------------
# Step 2 — Cortex Analyst REST
# -----------------------------------------------------------------------------
def call_cortex_analyst(
    session: Session,
    user_text: str,
    *,
    semantic_view: Optional[str] = None,
    timeout_sec: int = 30,
) -> dict[str, Any]:
    """
    POST /api/v2/cortex/analyst/message
    요청 본문은 공식 스펙: semantic_view (FQN). semantic_view_name 아님.
    """
    token, token_type = _get_rest_auth(session)
    url = _analyst_api_url(session)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if token_type:
        headers["X-Snowflake-Authorization-Token-Type"] = token_type

    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": user_text}],
            }
        ],
        "semantic_view": semantic_view or semantic_view_fqn(),
        "database": "HACKATHON_DB",
        "schema": "ANALYTICS",
    }

    resp = requests.post(url, headers=headers, json=body, timeout=timeout_sec)
    try:
        payload = resp.json()
    except Exception:  # noqa: BLE001
        payload = {"_raw_text": resp.text}

    if resp.status_code != 200:
        raise RuntimeError(
            f"Cortex Analyst HTTP {resp.status_code}: {json.dumps(payload, ensure_ascii=False)[:2000]}"
        )
    return payload


def extract_analyst_sql_and_text(payload: dict[str, Any]) -> tuple[Optional[str], str, list[str]]:
    """응답에서 SQL 문장, 분석가 설명 텍스트, 경고 목록 추출."""
    warnings: list[str] = []
    for w in payload.get("warnings") or []:
        if isinstance(w, dict) and w.get("message"):
            warnings.append(str(w["message"]))

    msg = payload.get("message") or {}
    contents = msg.get("content") or []
    texts: list[str] = []
    sql_stmt: Optional[str] = None

    for block in contents:
        if not isinstance(block, dict):
            continue
        t = block.get("type")
        if t == "text" and block.get("text"):
            texts.append(str(block["text"]))
        elif t == "sql" and block.get("statement"):
            sql_stmt = str(block["statement"]).strip()
        elif t == "suggestion":
            texts.append(str(block.get("suggestions", block)))

    return sql_stmt, "\n".join(texts), warnings


def run_analyst_sql(session: Session, sql: Optional[str]) -> list[dict[str, Any]]:
    """생성 SQL 실행 후 행을 dict 리스트로 반환."""
    if not sql or not sql.strip():
        return []
    try:
        rows = session.sql(sql).collect()
    except Exception as e:  # noqa: BLE001
        logger.exception("Analyst SQL 실행 실패")
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
def _find_numeric_keys(row: dict[str, Any]) -> dict[str, float]:
    res: dict[str, float] = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, (int, float)):
            try:
                res[str(k).upper()] = float(v)
            except (TypeError, ValueError):
                continue
    return res


def apply_rules(
    rows: list[dict[str, Any]],
    intent: dict[str, Any],
) -> dict[str, Any]:
    """
    결과 행이 있을 때만 수치 기반 휴리스틱.
    숫자가 없으면 명시적으로 데이터 없음 처리.
    """
    result: dict[str, Any] = {
        "has_data": bool(rows),
        "marketing": {},
        "funnel": {},
        "cs": {},
        "notes": [],
    }

    if not rows:
        result["notes"].append("Analyst SQL 결과가 없거나 실행 행이 0건입니다.")
        return result

    # 마케팅: CONTRACT_CVR, WEIGHTED_CONTRACT_CVR, contract_cvr 등
    best_ch = None
    best_cvr = -1.0
    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        cvr = None
        for key in (
            "CONTRACT_CVR",
            "WEIGHTED_CONTRACT_CVR",
            "WEIGHTEDCONTRACTCVR",
            "contract_cvr",
        ):
            if key in u and u[key] is not None:
                try:
                    cvr = float(u[key])
                    break
                except (TypeError, ValueError):
                    pass
        src = u.get("UTM_SOURCE") or u.get("utm_source")
        sessions = u.get("TOTAL_SESSIONS") or u.get("total_sessions")
        try:
            sessions = float(sessions) if sessions is not None else 0
        except (TypeError, ValueError):
            sessions = 0

        contracts = u.get("TOTAL_CONTRACTS") or u.get("total_contracts")
        try:
            contracts = float(contracts) if contracts is not None else 0
        except (TypeError, ValueError):
            contracts = 0

        if (cvr is not None and src is not None 
            and cvr > best_cvr 
            and sessions >= 100
            and contracts >= 10):
            best_cvr = cvr
            best_ch = str(src)

    if best_ch is not None:
        result["marketing"] = {
            "best_channel": best_ch,
            "cvr": best_cvr,
            "source": "sql_aggregate_max_cvr",
        }
    elif "marketing" in intent.get("domains", []):
        result["notes"].append("마케팅 CVR 비교에 쓸 수 있는 숫자 열을 결과에서 찾지 못했습니다.")

    # 퍼널: CVR_* 컬럼 중 최소값을 병목 후보로
    funnel_keys = (
        "CVR_CONSULT_REQUEST",
        "CVR_REGISTEND",
        "CVR_OPEN",
        "CVR_PAYEND",
    )
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
        stage = min(valid, key=valid.get)  # type: ignore[arg-type]
        label_map = {
            "CVR_CONSULT_REQUEST": "상담요청",
            "CVR_REGISTEND": "접수",
            "CVR_OPEN": "개통",
            "CVR_PAYEND": "지급",
        }
        result["funnel"] = {
            "stage": label_map.get(stage, stage),
            "cvr": valid[stage],
            "all_stage_cvrs": valid,
            "source": "min_mean_stage_cvr",
        }

    # CS: 연결률
    conn_rates: list[float] = []
    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        for key in ("CONNECTION_RATE", "WEIGHTED_CONNECTION_RATE", "AVG_REPORTED_CONNECTION_RATE"):
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
def cortex_complete(session: Session, prompt: str, model: str = CORTEX_MODEL) -> str:
    """
    snowflake.cortex.Complete 우선, 패키지 없으면 SNOWFLAKE.CORTEX.COMPLETE SQL 폴백.
    """
    try:
        from snowflake.cortex import Complete  # type: ignore[import-not-found]

        out = Complete(model, prompt, session=session)
        return out if isinstance(out, str) else str(out)
    except ImportError:
        logger.info("snowflake.cortex.Complete 미설치 — SQL 함수로 폴백")
    except Exception as e:  # noqa: BLE001
        logger.warning("Complete Python API 실패, SQL 폴백: %s", e)

    rows = session.sql(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)",
        params=[model, prompt],
    ).collect()
    if not rows:
        return ""
    val = rows[0][0]
    return val if isinstance(val, str) else str(val)


# -----------------------------------------------------------------------------
# Step 5 — 구조화 JSON
# -----------------------------------------------------------------------------
EMPTY_JSON_TEMPLATE = {
    "region_analysis": "",
    "marketing_recommendation": {
        "best_channel": "",
        "cvr": None,
        "action": "",
    },
    "funnel_bottleneck": {
        "stage": "",
        "cvr": None,
        "action": "",
    },
    "cs_insight": {
        "connection_rate": None,
        "peak_time": "",
        "action": "",
    },
    "action_items": [],
    "reasoning": "",
}


def build_llm_prompt(
    user_question: str,
    intent: dict[str, Any],
    analyst_text: str,
    sql_text: Optional[str],
    sql_rows: list[dict[str, Any]],
    rules: dict[str, Any],
) -> str:
    rows_json = json.dumps(sql_rows[:50], ensure_ascii=False, default=str)
    rules_json = json.dumps(rules, ensure_ascii=False, default=str)

    return f"""당신은 통신사 마케팅·영업·CS 통합 운영 어드바이저입니다.

{STATIC_CONTEXT}

[규칙 — 반드시 준수]
1) 사용자에게 보이는 모든 숫자(%, 건수, 금액 등)는 아래 "SQL 결과 행" 또는 "규칙 엔진 요약"에 실제로 있는 값만 사용하세요. 없으면 null 또는 "데이터 없음"으로 두고 추측하지 마세요.
2) 설명과 액션 추천은 자유롭게 작성하되, 수치 환각 금지.
3) 출력은 JSON 하나만 (다른 텍스트 없이). 아래 키 구조를 정확히 맞추세요.

출력 JSON 스키마:
{json.dumps(EMPTY_JSON_TEMPLATE, ensure_ascii=False, indent=2)}

[사용자 질문]
{user_question}

[의도 분석]
{json.dumps(intent, ensure_ascii=False)}

[Cortex Analyst 설명]
{analyst_text}

[생성된 SQL]
{sql_text or "(없음)"}

[SQL 결과 행 (최대 50행)]
{rows_json}

[규칙 엔진 요약]
{rules_json}

위 컨텍스트만 근거로 JSON을 작성하세요.
"""


def parse_llm_json(text: str) -> dict[str, Any]:
    text = text.strip()
    # 코드블록 제거 추가
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
    """규칙 엔진에 확실한 숫자가 있으면 LLM 필드를 보정."""
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
            ci["peak_time"] = "일요일 14~16시 (사전 인사이트; SQL에 시간대가 있으면 그에 맞게 조정)"
    return out


# -----------------------------------------------------------------------------
# 통합 실행
# -----------------------------------------------------------------------------
def run_agent(user_question: str) -> dict[str, Any]:
    """전체 파이프라인 실행 후 구조화 dict 반환."""
    session = get_session()
    intent = parse_intent(user_question)

    analyst_payload: dict[str, Any] = {}
    analyst_text = ""
    sql_stmt: Optional[str] = None
    warnings: list[str] = []

    try:
        analyst_payload = call_cortex_analyst(session, intent["analyst_query"])
        sql_stmt, analyst_text, warnings = extract_analyst_sql_and_text(analyst_payload)
    except Exception as e:  # noqa: BLE001
        logger.exception("Cortex Analyst 호출 실패")
        analyst_text = f"(Analyst 오류) {e}"

    sql_rows: list[dict[str, Any]] = []
    if sql_stmt:
        try:
            sql_rows = run_analyst_sql(session, sql_stmt)
        except Exception as e:  # noqa: BLE001
            analyst_text += f"\n(SQL 실행 오류) {e}"

    rules = apply_rules(sql_rows, intent)
    prompt = build_llm_prompt(
        user_question,
        intent,
        analyst_text,
        sql_stmt,
        sql_rows,
        rules,
    )

    try:
        llm_raw = cortex_complete(session, prompt)
    except Exception as e:  # noqa: BLE001
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
        "warnings": warnings,
        "rules": rules,
    }
    return merged


# -----------------------------------------------------------------------------
# 데모
# -----------------------------------------------------------------------------
DEMO_QUESTIONS = [
    "다음달 강남구 마케팅 어떻게 해야 해?",
]


if __name__ == "__main__":
    if load_dotenv:
        load_dotenv()

    print("=== 통합 에이전트 데모 (Cortex Analyst + Complete) ===\n")
    for i, q in enumerate(DEMO_QUESTIONS, 1):
        print(f"--- 질문 {i} ---\n{q}\n")
        try:
            out = run_agent(q)
            print(json.dumps(out, ensure_ascii=False, indent=2))
        except Exception as exc:  # noqa: BLE001
            print(f"오류: {exc}", file=sys.stderr)
            sys.exit(1)
        print("\n")
