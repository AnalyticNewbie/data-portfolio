🏠 Ames Housing Price Prediction

Advanced Regression Techniques (Kaggle)

Overview

This project tackles the Kaggle House Prices: Advanced Regression Techniques competition using a disciplined, validation-first approach. Rather than relying on aggressive feature churn or leaderboard-specific tricks, the focus was on building a robust, generalisable model through careful preprocessing, residual analysis, and targeted hyperparameter optimisation.

The final solution achieved a public leaderboard score of 0.12425, ranking ~top 10–15%, using a single, well-tuned CatBoost model.

Problem Framing

The dataset contains ~1,460 training examples with a mix of:

numeric and categorical features,

strong non-linear relationships,

skewed target distribution,

and a small number of extreme outliers.

The evaluation metric (RMSLE) strongly penalises systematic over- and under-prediction, especially for high-priced homes. This makes generalisation and tail behaviour more important than raw in-sample fit.

Approach
1. Data Preparation & Baseline Engineering

Domain-aware missing value handling

Log-transformation of the target (SalePrice)

Skewness correction for numeric features

Ordinal encoding for quality-related variables

Conservative outlier removal (extreme size / low price cases)

This produced a strong baseline without introducing leakage.

2. Validation Discipline

A consistent 5-fold cross-validation strategy was used throughout:

Frozen folds for all tuning and comparisons

All decisions driven by out-of-fold (OOF) performance

Public leaderboard used only as a sanity check, not an optimisation target

This prevented common CV ↔ LB divergence issues.

3. Residual-Driven Analysis (What Not to Add)

Residual plots and row-level error inspection revealed:

systematic errors in rare cases (e.g. abnormal sales, large low-quality houses)

tempting opportunities for “correction” features

Several residual-based features (e.g. neighbourhood encodings, binary “hook” flags) were tested.

Key learning:
Although these features appeared intuitive, they reduced generalisation and degraded leaderboard performance by increasing variance. They were deliberately removed.

This step was critical in avoiding overfitting.

4. Hyperparameter Optimisation (The Real Gain)

The largest improvement came not from new features, but from rigorous model regularisation.

Using Optuna, CatBoost was tuned with:

frozen CV folds

early stopping

Bayesian bootstrap

explicit control of depth, regularisation, and randomness

Key tuned parameters:

shallow trees (depth = 4)

moderate l2_leaf_reg

Bayesian bagging with tuned bagging_temperature

controlled random_strength

This reduced tail volatility and aligned CV with leaderboard performance.

Final Model

Model: CatBoost Regressor

Target: log1p(SalePrice)

Validation: 5-fold CV (frozen)

Tuning: Optuna (OOF RMSE objective)

Submission: single-model (no stacking)

Results

Best CV RMSE: ~0.112

Public LB score: 0.12425

Rank: ~935

Notably, this outperformed:

stacked ensembles

feature-heavy variants

neighbourhood target encoding approaches

Key Learnings
1. Simpler Models Generalise Better

Well-regularised, shallow tree ensembles often outperform complex stacks on small tabular datasets.

2. Feature Churn Is Risky

More features ≠ better models. Several “reasonable” ideas actively hurt performance.

3. Residual Analysis Is Diagnostic, Not Prescriptive

Residuals are excellent for understanding model behaviour—but not every pattern should become a feature.

4. Hyperparameters Matter More Than Architecture

Proper regularisation delivered the largest single improvement late in the project.

What I Would Do Next (If Needed)

Tune XGBoost with the same discipline

Attempt a very light blend (e.g. 80% CatBoost / 20% XGB)

Otherwise, lock this solution as final
