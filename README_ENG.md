# 🏢 Telecom Operations Decision AI Agent

## 🧠 AI That Directly Decides Your Marketing Budget
Automatically selects where to spend money across 173 channels

> Integrates Marketing / Sales / CS data to make a single unified decision

> Not just analysis — an AI agent that determines **what to prioritize first** from multiple data sources

> Top-1 Accuracy 33% | Hallucination Rate 0% | CVR Uplift 3.9x

---

## ❗ Problem

Telecom/rental companies have their Marketing, Sales, and CS teams **each making decisions from siloed data.**

```
Marketing  →  Only sees GA4 channel performance
Sales      →  Only sees regional contract numbers
CS         →  Only sees call center connection rates
```

This fragmentation causes:
- Marketing budget wasted on wrong channels
- CS understaffing/overstaffing leading to customer churn
- Missed funnel bottlenecks causing conversion rate drops
- **→ Real revenue loss**

---

## 🖥️ Demo UI

3 questions covering decisions across Marketing, Funnel, and CS.

### 1) Marketing Decision
<img src="./assets/marketing_demo.gif" width="800"/>

### 2) Funnel Bottleneck Detection
<img src="./assets/funnel_demo.png" width="800"/>

### 3) CS Optimization Insight
![cs demo](./assets/demo_cs.png)

> **Demo UI = Interface** / **agent_v2.py = Brain**
>
> The Snowflake Demo UI is a stable interface built on verified SQL scenarios.
> The core decision-making logic runs in `agent_v2.py` as a multi-domain reasoning engine.

---

## 🎯 Example Insight

**Question:** "Which marketing channel is the most efficient?"

**Result:**
```
Best Channel:      nc_money / direct_ps
CVR:               27.1%
vs. Full Average:  +289.8%
Rank:              Top-3 among 173 channels
Revenue:           ₩30,651,037 / 872 sessions
```

**Insight:**
- Direct traffic brings high-intent customers, resulting in higher CVR
- Composite analysis considering both CVR and revenue contribution

**Action:**
- Run A/B test with 20~30% budget increase on this channel
- Discover and scale similar direct-type channels
- Investigate funnel drop-off at registration → activation step

---

## 🧠 Advanced Decision Engine (`agent_v2.py`)

The core of this project is not a simple Q&A bot — it's a **multi-domain decision agent.**

```
Demo UI (Stable Layer)            agent_v2.py (Decision Engine)
──────────────────────    vs    ────────────────────────────────
3 verified scenarios             Free-form natural language input
Single domain response           Marketing + Funnel + CS in parallel
Hardcoded SQL                    Dynamic SQL via Cortex Analyst
Stability-first                  Decision quality-first
```

What `agent_v2.py` does:

- **Multi-domain parallel analysis** — Marketing / Funnel / CS data at once
- **Conflict Detection** — Auto-detects "good inflow but poor conversion"
- **Impact-based prioritization** — Auto-ranks 1st through 3rd priority
- **Actionable recommendations** — Generates specific next steps automatically

### 🧠 Multi-Domain Decision Examples

**Question 1: Priority Decision**
> What should we fix first to improve performance?

<img src="./assets/multi_domain_demo_1.gif" width="800"/>

**Result:**
```
Priority 1 [Funnel]    Fix consultation stage bottleneck (Impact: High)
Priority 2 [Marketing] 20~30% budget increase A/B test for nc_money
Priority 3 [CS]        Proactive monitoring on Friday 7AM
```

> `agent_v2.py` simultaneously analyzes Marketing / Funnel / CS data to produce
> direct causes, indirect causes, conflict detection, and prioritized actions.

---

## 📈 Impact

| | Before | AI Agent |
|--|--------|----------|
| Data Integration | Siloed by department | Marketing + Sales + CS unified |
| Decision Time | Hours | Minutes |
| Channel Comparison | Relies on experience | Quantitative comparison of all 173 |
| Action Generation | After meetings | Immediately actionable |

→ **Faster budget reallocation → Revenue optimization**

---

## ⚠️ Baseline vs Ours

**LLM Only approach:**
```
Selects highest CVR channel
→ tips_capsule (CVR 34.7%, 1,031 sessions)
→ Ignores traffic volume / revenue → Wrong recommendation
```

**Our System:**
```
CVR × 0.4 + Revenue per Session × 0.6 composite score
→ nc_money/direct_ps (CVR 27.1%, 872 sessions)
→ Reflects actual revenue contribution → Correct recommendation
```

> Rule-based grounding removes LLM hallucination and corrects output with real data

> → This difference was confirmed in evaluation: our system outperforms CVR-only baseline in recommendation accuracy.

---

## 📊 Evaluation (KPI-based Validation)

This project is not just a demo — it is quantitatively validated across recommendation accuracy, numeric grounding, and business impact.

### Evaluation Criteria

| Metric | Indicator | Meaning |
|--------|-----------|---------|
| **Recommendation Accuracy** | Top-1 / Top-3 Accuracy | Did AI correctly select the optimal channel? |
| **Numeric Grounding** | Hallucination Rate (threshold: 2%p) | Does LLM output match real data? |
| **Business Impact** | CVR Uplift / Revenue Uplift | How much better is the recommended channel vs. average? |

### Results Summary

| Metric | Result |
|--------|--------|
| Top-1 Accuracy | 33% (1/3 cases) |
| Top-3 Accuracy | 67% (2/3 cases) |
| Hallucination Rate | **0%** (threshold: 2%p) |
| CVR Uplift | **3.9x** (recommended vs. average) |
| Revenue Uplift | **4.4x** |

### Baseline Comparison

| Method | Top-1 Accuracy | Note |
|--------|----------------|------|
| Random | ~1% | Random selection |
| CVR Only | **0%** | Overestimates low-volume channels |
| **Our System** | **33%** | CVR + Revenue composite score |

> CVR-only selection overestimates low-volume channels.
> Our system corrects this by incorporating revenue, yielding more stable recommendations.

> ⚠️ Evaluation is based on offline data and does not guarantee actual revenue increase.

---

## 🎬 Demo Flow

```
1. Natural language question input
   "Which marketing channel is the most efficient?"
        ↓
2. Cortex Analyst → Auto-generates SQL / Verified SQL in demo
   (Queries all 173 channels)
        ↓
3. Snowpark → Real-time data retrieval
   (2,621 records analyzed live)
        ↓
4. Rule Engine → Quantitative analysis
   CVR + Revenue composite score
   Rank vs. full channel average
        ↓
5. Cortex Complete → Insight generation
   Channel characteristic causal explanation
   3 concrete action items
        ↓
6. Streamlit → Decision UI output
```

---

## 🧠 Core Reasoning Stack

This project defines a YAML-based Semantic View on top of V01/V03/V07/V09/V10 views,
enabling Cortex Analyst to understand domain semantics and generate accurate SQL.

`agent_v2.py` is not a single-query responder — it simultaneously interprets
Marketing / Funnel / CS / Sales data to produce:

- **Direct Cause** — Bottlenecks confirmed by data
- **Indirect Cause** — Cross-domain inference
- **Priority** — Ranked by impact / urgency / difficulty
- **Action** — Specific, immediately executable steps

While the Snowflake Streamlit demo uses verified SQL for stability,
the full system is designed to support free-form Cortex Analyst queries.

---

## 🏗️ Architecture

### 1) Stable Demo Layer (Snowflake Streamlit)
```
Verified Questions (3 scenarios)
        ↓
Cortex Analyst → Verified SQL
        ↓
Snowpark execution
        ↓
Rule Engine (CVR + Revenue composite analysis)
        ↓
Cortex Complete → Insight generation
        ↓
Streamlit in Snowflake UI
```

### 2) Advanced Reasoning Layer (Local `agent_v2.py`)
```
Natural Language Question
        ↓
Decision Type Classification (root_cause / priority / budget / ops)
        ↓
Multi-Domain Evidence Extraction (Marketing + Funnel + CS in parallel)
        ↓
Conflict Detection (cross-domain signal collision)
        ↓
Priority Ranking (impact / urgency / difficulty)
        ↓
Cortex Complete Synthesis
        ↓
Final Decision Output (direct cause / indirect cause / priority / action)
```

---

## ❄️ Why Snowflake Cortex?

| Feature | Role |
|---------|------|
| **Cortex Analyst** | Natural language → Auto SQL generation |
| **Cortex Complete** | Data-grounded insight + action generation |
| **Snowpark** | Real-time large-scale data retrieval |
| **Streamlit in Snowflake** | Self-contained UI on the platform |

→ **Not just an LLM — a data AI system that runs end-to-end on Snowflake**

---

## 💡 Why This Is Different

- Simple recommendation ❌ → **Rank-based decisions across all 173 channels** ✅
- Simple LLM ❌ → **Hallucination removed via rule-based grounding** ✅
- Simple analysis ❌ → **3 immediately actionable items auto-generated** ✅
- Single department ❌ → **Marketing + Sales + CS unified analysis** ✅
- Simple demo ❌ → **Quantitatively validated system** ✅

---

## 💰 Why It Matters

Spending budget on the wrong channels leads to:
- Wasted ad spend (concentrated on low-CVR channels)
- Conversion rate decline (funnel bottlenecks unaddressed)
- Customer churn (CS connection rate below target)

**This system:**
- Automatically identifies high-intent customer channels
- Recommends ROI-based budget reallocation
- Supports revenue-maximizing decision-making

---

## 🔥 Key Insights (EDA)

| Domain | Finding | Impact |
|--------|---------|--------|
| 📊 Marketing | Up to **4,000x CVR gap** between channels | Immediate effect from budget reallocation |
| ⚠️ Funnel | **Registration→Activation** is the largest bottleneck | Conversion rate improvement via process fix |
| 📞 CS | Inbound connection rate **55.8%**, below 70% target | Workforce allocation optimization needed |

---

## 🤖 Agent Pipeline (`agent_v2.py`)

```python
run_agent(question)
  → parse_intent()              # Decision type classification
                                #   root_cause / priority / budget / ops / regional
  → call_cortex_analyst()       # Per-domain SQL auto-generation
      marketing → V07 channel analysis
      funnel    → V03 funnel analysis
      cs        → V10 hourly analysis

  → extract_*_evidence()        # Domain-structured evidence extraction
      marketing_evidence → best_channel, cvr, rank, signal
      funnel_evidence    → bottleneck_stage, dropoff, signal
      cs_evidence        → connection_rate, peak_time, signal

  → detect_conflicts()          # Cross-domain signal conflict detection
      "Marketing 🟢 + Funnel 🔴 → Good inflow, conversion lost"

  → prioritize_actions()        # Impact/urgency/difficulty-based ranking
      Priority 1: Funnel bottleneck
      Priority 2: Marketing budget expansion
      Priority 3: CS optimization

  → cortex_complete()           # Final LLM synthesis
  → parse_json()                # Direct cause / indirect cause / action structured output
```

---

## 📂 File Structure

```
telecom-ops-agent/
├── eda_final.ipynb        # Data exploration & key insight derivation
├── semantic_model.yaml    # Cortex Analyst Semantic View definition (5 views)
├── agent.py               # Local basic Analyst + Complete pipeline
├── agent_v2.py            # Multi-domain decision agent ★
│                          #   (reasoning / conflict detection / priority ranking)
├── streamlit_app.py       # Local Streamlit (connected to agent_v2)
├── snowflake_app.py       # Streamlit in Snowflake demo app (verified scenarios)
├── eval.py                # KPI-based system evaluation (Accuracy / Hallucination / Uplift)
└── README.md
```

---

## 🛠️ Tech Stack

- **Snowflake Cortex Analyst** — Natural language → Auto SQL generation
- **Snowflake Cortex Complete** — mistral-large2 insight generation
- **Streamlit in Snowflake** — Integrated operations dashboard
- **Python / Snowpark** — Rule-based decision engine

---

## 📊 Dataset

**AJeongDang — Korea Telecom Subscription Analytics** (Snowflake Marketplace)

| View | Description | Usage |
|------|-------------|-------|
| V01 | Monthly/regional contract stats | Sales trend analysis |
| V03 | Funnel stage conversion rates | Bottleneck detection |
| V07 | GA4 marketing channel performance | Channel optimization |
| V09 | Monthly call center stats | CS performance tracking |
| V10 | Hourly call distribution | Workforce allocation |

---

## 🚀 How to Run

### Local
```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

### Snowflake Streamlit
```
Snowsight → Projects → Streamlit → New App
Paste snowflake_app.py content → Run
```

---

## 🎤 Verified Demo Scenarios

- "Which marketing channel is the most efficient?"
- "What is the biggest bottleneck dropping funnel conversion for rentals?"
- "When is the CS connection rate lowest, and how should we improve it?"

> These 3 scenarios are reliably reproducible in the Snowflake Streamlit demo.

---

## 🧪 Demo Setup

In the local environment, a YAML-based Semantic View was configured and
multi-domain reasoning flows were validated through `agent_v2.py`.

In the Snowflake Streamlit demo, verified SQL scenarios are used for stability and reproducibility.

**The demo app is a stable product interface.
`agent_v2.py` is the advanced agent containing the project's core decision-making logic.**

---

## 🚀 Current Implementation + Extensions

**Already implemented (`agent_v2.py`)**
- Multi-domain simultaneous analysis (Marketing + Funnel + CS)
- Automatic decision type classification
- Conflict Detection and cross-domain causal inference
- Impact/urgency-based prioritization

**Extensible to**
- Cortex ML Forecast integration → "What to prepare for next month"
- Region-level decisions → Area-specific action recommendations
- Operations automation → Auto-deliver recommendations as alerts/reports

---

## 🔒 Data Usage Notice

This repository does not contain raw hackathon data.
Only code and architecture are published. Actual data is not shared externally per hackathon rules.

---

## 📅 Development Period
April 2026 | Snowflake Hackathon 2026
