# 🏢 통신 운영 의사결정 AI 에이전트

## 🧠 AI가 마케팅 예산을 직접 결정합니다

173개 채널 중 어디에 돈을 써야 하는지 자동으로 선택하는 AI

> 마케팅 / 영업 / CS 데이터를 통합해 하나의 의사결정을 내리는 AI

---

## 🖥️ Demo UI

> 스크린샷 / GIF 추가 예정







---

## ❗ Problem

렌탈/통신 업체의 마케팅·영업·CS 부서는 **각각 따로 데이터를 보고 의사결정**합니다.

```
마케팅팀  →  GA4 채널 성과만 봄
영업팀    →  지역별 계약 수만 봄
CS팀      →  콜센터 연결률만 봄
```

이 단절이 만드는 문제:

- 마케팅 예산을 잘못된 채널에 낭비
- CS 인력 과소/과다 배치로 고객 이탈
- 퍼널 병목을 몰라서 계약 전환율 감소
- **→ 실제 매출 손실로 이어짐**

---

## 🎯 Example Insight

**질문:** "어떤 마케팅 채널이 제일 효율적이야?"

**결과:**

```
최적 채널: nc_money / direct_ps
CVR:       27.1%
전체 평균 대비: +289.8%
173개 채널 중: 2위
매출 기여:  30,651,037원 / 872세션
```

**인사이트:**

- direct 유입 특성상 고의도 고객 비중이 높아 CVR이 높게 나타남
- CVR + 매출 기여도를 동시에 고려한 복합 분석 결과

**Action:**

- 해당 채널 예산 20~30% 확대 A/B 테스트 진행
- 유사한 direct 기반 채널 발굴 및 확장
- 퍼널 접수→개통 단계 이탈 원인 분석



또한 동일 시스템으로:  
- "계약 과정에서 어디서 고객이 이탈해?"  
- "콜센터 연결률이 낮은 시간대는?"  
  
와 같은 질문도 통합 분석 가능합니다.

---

## 📈 Impact


|         | 기존 방식     | AI 에이전트         |
| ------- | --------- | --------------- |
| 데이터 통합  | 부서별 분리    | 마케팅+영업+CS 통합    |
| 의사결정 시간 | 수 시간      | 수 분             |
| 채널 비교   | 담당자 경험 의존 | 173개 전체 정량 비교   |
| 액션 도출   | 회의 후 결정   | 즉시 실행 가능한 액션 제공 |


→ **빠른 예산 재배분 → 매출 최적화**

---

## ⚠️ Baseline vs Ours

**LLM Only 방식:**

```
가장 높은 CVR 채널 선택
→ tips_capsule (CVR 34.7%, 세션 1,031건)
→ 트래픽/매출 고려 없음 → 잘못된 추천
```

**Our System:**

```
CVR 40% + Revenue per Session 60% 복합 score
→ nc_money/direct_ps (CVR 27.1%, 세션 872건)
→ 실제 매출 기여도 반영 → 올바른 추천
```

> Rule-based grounding으로 LLM 환각을 제거하고 실제 데이터 기반으로 보정

---

## 🎬 Demo Flow

```
1. 자연어 질문 입력
   "어떤 마케팅 채널이 제일 효율적이야?"
        ↓
2. Cortex Analyst → SQL 자동 생성
   (173개 채널 전체 조회)
        ↓
3. Snowpark → 실제 데이터 조회
   (2,621건 실시간 분석)
        ↓
4. Rule Engine → 정량 분석
   CVR + Revenue 복합 score 계산
   전체 평균 대비 순위 산출
        ↓
5. Cortex Complete → 인사이트 생성
   채널 특성 인과 설명
   구체적 액션 3개 제공
        ↓
6. Streamlit → 의사결정 UI 출력
```

---

## 🏗️ Architecture

```
V01 지역계약  ┐
V03 퍼널      ├→ Semantic View → Cortex Analyst → SQL 생성
V07 마케팅    │                      ↓
V09 월별콜    │              Snowpark 실행 (실제 데이터)
V10 시간대콜  ┘                      ↓
                           Rule Engine (CVR + Revenue)
                                      ↓
                           Cortex Complete (설명 + 액션)
                                      ↓
                           Streamlit in Snowflake (UI)
```

---

## ❄️ Why Snowflake Cortex?


| 기능                         | 역할                  |
| -------------------------- | ------------------- |
| **Cortex Analyst**         | 자연어 질문 → SQL 자동 생성  |
| **Cortex Complete**        | 데이터 기반 인사이트 + 액션 생성 |
| **Snowpark**               | 대규모 데이터 실시간 조회      |
| **Streamlit in Snowflake** | 플랫폼 내 완결된 UI        |


→ **단순 LLM이 아닌 "Snowflake 위에서 완결되는 데이터 AI 시스템"**

---

## 💡 Why This Is Different

- 단순 추천 ❌ → **173개 채널 전체 대비 순위 기반 의사결정** ✅
- 단순 LLM ❌ → **Rule-based grounding으로 hallucination 제거** ✅
- 단순 분석 ❌ → **즉시 실행 가능한 Action 3개 자동 제공** ✅
- 단일 부서 ❌ → **마케팅 + 영업 + CS 통합 분석** ✅

---

## 💰 Why It Matters

잘못된 채널에 예산을 쓰면:

- 광고비 낭비 (CVR 낮은 채널에 집중)
- 전환율 감소 (퍼널 병목 방치)
- 고객 이탈 (CS 연결률 미달)

**이 시스템은:**

- 고의도 고객 채널 자동 식별
- ROI 기반 예산 재배분 제안
- 매출 극대화 의사결정 지원

---

## 🔥 핵심 인사이트 (EDA)


| 도메인    | 발견                          | 임팩트              |
| ------ | --------------------------- | ---------------- |
| 📊 마케팅 | 채널별 CVR **최대 4,000배 격차**    | 예산 재배분으로 즉시 효과   |
| ⚠️ 퍼널  | **"접수→개통" 단계** 최대 이탈 병목     | 프로세스 개선으로 전환율 향상 |
| 📞 CS  | 수신 연결률 **55.8%**, 목표 70% 미달 | 인력 배치 최적화 필요     |


---

## 🤖 에이전트 파이프라인

```python
run_agent(질문)
  → parse_intent()           # 도메인 파악 (마케팅/영업/CS)
  → call_cortex_analyst()    # 자연어 → SQL 자동 생성
  → run_analyst_sql()        # 실제 데이터 조회
  → apply_rules()            # Rule-based 판단
      - CVR 40% + Revenue 60% 복합 score
      - 전체 평균 대비 +289.8%, 173개 중 2위
      - 채널 특성 인과 추론 (direct_ps → 고의도)
      - CS 목표 대비 gap 계산
  → cortex_complete()        # LLM 설명 + 액션 생성
  → merge_grounding()        # Rule 숫자로 LLM 보정 (환각 방지)
```

---

## 📂 파일 구조

```
telecom-ops-agent/
├── eda_final.ipynb       # 데이터 탐색 및 핵심 인사이트 도출
├── semantic_model.yaml   # Cortex Analyst 시맨틱 모델 (뷰 5개, verified query 5개)
├── agent.py              # 에이전트 핵심 로직 (로컬)
├── streamlit_app.py      # Streamlit UI (로컬)
├── snowflake_app.py      # Streamlit in Snowflake (배포용)
└── README.md
```

---

## 🛠️ Tech Stack

- **Snowflake Cortex Analyst** — 자연어 → SQL 자동 생성
- **Snowflake Cortex Complete** — mistral-large2 인사이트 생성
- **Streamlit in Snowflake** — 통합 운영 대시보드
- **Python / Snowpark** — Rule-based 판단 엔진

---

## 📊 데이터셋

**아정당 — 한국 통신 구독·계약 분석 데이터** (Snowflake Marketplace)


| 뷰   | 설명             | 활용        |
| --- | -------------- | --------- |
| V01 | 월별·지역별 계약 통계   | 영업 트렌드 분석 |
| V03 | 계약 퍼널 단계별 전환율  | 병목 탐지     |
| V07 | GA4 마케팅 채널별 성과 | 채널 최적화    |
| V09 | 월별 콜센터 통계      | CS 성과 추적  |
| V10 | 시간대별 콜 분포      | 인력 배치 최적화 |


---

## 🚀 실행 방법

### 로컬

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

### Snowflake Streamlit

```
Snowsight → Projects → Streamlit → New App
snowflake_app.py 내용 붙여넣기 → Run
```

---

## 📅 개발 기간

2026년 4월 | Snowflake Hackathon 2026