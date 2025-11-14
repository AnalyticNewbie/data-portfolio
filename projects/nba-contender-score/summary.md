I recently ran a  data analysis project testing a fan metric used to gauge NBA championship potential: the *ContenderScore*, which combines Offensive (*OffRank*) and Defensive (*DefRank*) efficiency rankings.

🧠 The Hypothesis

The core question was: Does the *ContenderScore* accurately predict a team's probability of being a true contender (Conference Finals or better)?

📊 The Data & Model

+ Data Set: Every NBA team from the 2005 season through 2018 (the 30-team era available in the dataset)

+ Metric Tested: $\text{ContenderScore} = 10 \times \left( \frac{(30 – \text{OffRank}) + (30 – \text{DefRank})}{60} \right)$

+ Model: **Logistic Regression**, designed to predict the probability of a binary outcome (Contender = 1, Not Contender = 0).

✅ The Results: High Accuracy, Conservative Predictions

+ The model found that the *ContenderScore* is a highly significant and powerful single predictor of playoff success.
+ The relationship is not random; the score is a statistically valid predictor. (**P < 0.001**)
+ The model correctly classified **89.5%** of all 420 teams (contenders and non-contenders).
+ For every 1-unit increase in the score, the odds of making the Conference Finals increase by over **3.3 times**.
+ When the model predicted a team was a contender, it was right nearly **two-thirds** of the time.

🔍 Key Takeaway on Prediction

The model is incredibly effective at identifying non-contenders (357 teams correctly predicted to fall short).
However, it is very conservative:
+ It only correctly identified **36.5%** (19 out of 52) of the teams that actually reached the Conference Finals or better.
+ This is highly precise when it predicts a contender, but it misses a lot of the actual contenders who snuck through with lower scores.

Bottom Line: The formula is great at ruling teams out and is a powerful signal for who the true top-tier threats

Potential Follow Up Analysis

+ Is an increase in *OffRank* or *DefRank* more impactful
+ Is there a point in the season where this score has a higher probability of accuracy?
+ How do other factors outside the teams performance factor in? e.g. injury, change of coach
