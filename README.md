# Self-Improvement Dashboard

A comprehensive personal tracking and gamification system for sleep, exercise, projects, and finance.

## Features

### 🎯 Identity-Based Gamification
- **XP & Levels**: Earn XP for every action, level up in 4 domains (Sleep, Exercise, Projects, Finance)
- **Daily Quests**: Optional challenges that push you beyond your baseline
- **Achievements & Streaks**: Build consistency and celebrate milestones
- **Identity Statements**: See yourself becoming who you want to be

### 📊 Comprehensive Tracking
- **Sleep**: Log bedtime, wake time, energy, quality with automatic XP for on-time sleep
- **Exercise**: Track resistance training (push-ups, pull-ups, etc.) and cardio with intensity-based XP
- **Projects**: Manage active projects with session logging and completion bonuses
- **Finance**: Track fortnightly finances, log decisions, monitor savings rate

### 🔍 Pattern Discovery
- **Automated Correlations**: Discover relationships between your metrics
- **Personal Bests**: Track your records across all domains
- **Trends Visualization**: See your progress over time
- **Multi-Domain Insights**: Understand how different areas of life affect each other

### 💡 Smart Features
- **Affirmations**: Identity-reinforcing statements based on your actual behavior
- **Experiments Framework**: Test hypotheses about what works for you
- **Energy ROI Tracking**: Learn what gives and drains your energy
- **Decision Logging**: Track financial decisions and their outcomes

## Quick Start

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd self-improvement-dashboard

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your preferences
```

### Run Locally

```bash
# From the project root
python app/app.py

# Open browser to http://localhost:8050
```

The app will automatically create the database on first run.

## Usage Guide

### Daily Workflow

**Morning (2 minutes):**
1. Open the app → Summary page shows your progress
2. Log yesterday's sleep (bedtime, wake time, energy)
3. Check your daily quests

**Throughout the day:**
- Log exercise sessions as you complete them
- Quick project session logs (date + duration + what you did)

**Evening (1 minute):**
- Review XP earned today
- Check if you completed any quests

**Weekly (5 minutes):**
- Review the Insights page for new patterns
- Plan experiments based on discoveries

### XP System

**Sleep (max 150 XP/day):**
- Bed on time: 50 XP
- Wake on time: 50 XP
- Bed routine completed: +25 XP
- Wake routine completed: +25 XP

**Exercise:**
- **Resistance**: 
  - Push-ups: 1 XP/rep
  - Pull-ups: 2 XP/rep
  - Squats: 0.5 XP/rep
  - Intensity ≥4: 1.5x multiplier
- **Cardio**:
  - Normal: 2 XP/min
  - Intense: 5 XP/min
  - Distance bonus: +10 XP/km

**Projects:**
- 1 XP per minute of work
- Completion bonus: duration × importance × 0.5
- Micro-output documented: +20 XP

**Finance:**
- Meet budget: 100 XP
- Savings: 0.25 XP per dollar saved
- Decision logged: 50 XP

### Identity Levels

Each domain has 5 levels:

**Sleep:**
1. Novice Sleeper (0 XP)
2. Consistent Sleeper (1,000 XP)
3. Sleep Optimizer (3,000 XP)
4. Sleep Master (6,000 XP)
5. Sleep Architect (10,000 XP)

**Exercise:**
1. Beginner (0 XP)
2. Regular Athlete (1,500 XP)
3. Disciplined Trainer (4,000 XP)
4. Peak Performer (8,000 XP)
5. Elite Athlete (15,000 XP)

**Projects:**
1. Dabbler (0 XP)
2. Focused Worker (2,000 XP)
3. Deep Work Practitioner (5,000 XP)
4. Productivity Master (10,000 XP)
5. Flow State Virtuoso (20,000 XP)

**Finance:**
1. Awareness (0 XP)
2. Intentional Spender (1,000 XP)
3. Strategic Saver (3,000 XP)
4. Financial Architect (7,000 XP)
5. Wealth Builder (15,000 XP)

## Navigation

- **Summary**: Home page with identity progress, daily quests, quick stats
- **Sleep**: Log sleep and view sleep trends
- **Exercise**: Track resistance training and cardio
- **Projects**: Manage projects and log work sessions
- **Finance**: Log fortnights and financial decisions
- **Insights**: Discover patterns, view correlations, track personal bests

## Data & Privacy

- All data is stored locally in SQLite (`data/dashboard.db`)
- No data leaves your machine unless you deploy it
- Regular backups recommended (see deployment guide)

## Customization

### Change Target Times
Edit values in the database:
```sql
UPDATE app_config SET value = '23:00' WHERE key = 'target_bedtime';
UPDATE app_config SET value = '07:00' WHERE key = 'target_wake_time';
```

### Adjust XP Values
Edit `app/analytics/scoring.py` to modify XP calculations.

### Add Exercise Types
Modify the dropdown options in `app/pages/exercise.py`.

## Deployment

See deployment guide in docs for:
- Fly.io deployment (recommended)
- PythonAnywhere
- Self-hosted options

## Development

### Project Structure
```
app/
├── app.py              # Main application
├── pages/              # Page components
│   ├── summary.py
│   ├── sleep.py
│   ├── exercise.py
│   ├── projects.py
│   ├── finance.py
│   └── insights.py
├── database/           # Database layer
│   ├── schema.sql
│   └── db.py
├── analytics/          # XP calculations
│   └── scoring.py
└── utils/              # Utilities
    └── affirmations.py
```

### Contributing

This is a personal project, but feel free to fork and adapt for your own use!

## Troubleshooting

**Database errors on first run:**
```bash
python -c "from app.database.db import init_db; init_db()"
```

**Port already in use:**
Edit `.env` and change `PORT=8050` to another port.

**Module import errors:**
Make sure you're running from the project root:
```bash
cd self-improvement-dashboard
python app/app.py
```

## License

MIT License - use and modify as you wish!

## Acknowledgments

Built with Dash, Plotly, and SQLite.
Inspired by identity-based behavior change principles.
