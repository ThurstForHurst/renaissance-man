"""App-wide configuration knobs."""

# DAG engagement tuning
NEAR_MISS_THRESHOLD = 0.80
BONUS_PROB_MULTIPLIER = 1.0
TEASER_DELAY_DEFAULT_HOURS = 24
MAX_TEASERS_SHOWN = 5
BONUS_XP_REWARD = 50

# Frontier sorting weights (used where weighted ranking is needed)
FRONTIER_SORT_WEIGHTS = {
    "near_miss": 2.0,
    "progress": 1.0,
    "tier": -0.2,
}
