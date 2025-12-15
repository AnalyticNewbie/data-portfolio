# 🏠 Ames Housing Price Prediction  
*Kaggle: House Prices – Advanced Regression Techniques*

## Overview

This project tackles the Kaggle **House Prices: Advanced Regression Techniques** competition using a **validation-first, generalisation-focused approach**.  
Rather than relying on aggressive feature churn or leaderboard-specific tricks, the emphasis was on **robust preprocessing, residual analysis, and disciplined hyperparameter tuning**.

**Final result:**  
- **Public score:** `0.12425`  
- **Rank:** ~`935`  
- **Model:** Single Optuna-tuned **CatBoost**

---

## Problem Framing

The dataset contains ~1,460 training observations with:
- mixed numerical and categorical features,
- a heavily skewed target (`SalePrice`),
- and a small number of extreme outliers.

Because the evaluation metric is **RMSLE**, errors on higher-priced houses and systematic bias matter more than raw in-sample fit. This makes **generalisation and tail behaviour** critical.

---

## Approach

### 1. Data Preparation
- Domain-aware missing value handling  
- Log-transformation of the target (`log1p(SalePrice)`)  
- Skewness correction for numeric features  
- Ordinal encoding for quality-based variables  
- Conservative outlier removal  

This produced a strong baseline without introducing leakage.

---

### 2. Validation Discipline
- **5-fold cross-validation** used consistently
- **Frozen CV splits** for all comparisons and tuning
- Decisions driven by **out-of-fold (OOF)** performance  
- Public leaderboard used only as a *sanity check*, not an optimisation target

This avoided common CV ↔ leaderboard divergence traps.

---

### 3. Residual-Driven Analysis (What *Not* to Add)

Residual plots and row-level inspection were used to understand model failure modes (e.g. abnormal sales, large low-quality houses).

Several intuitive feature ideas were tested (e.g. neighbourhood encodings, binary “hook” flags).

> **Key learning:**  
> Although these features appeared reasonable, they **reduced generalisation** and degraded l
