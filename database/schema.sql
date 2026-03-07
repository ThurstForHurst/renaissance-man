-- Sleep tracking
CREATE TABLE IF NOT EXISTS sleep_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL UNIQUE,
  bedtime TEXT,
  wake_time TEXT,
  target_bedtime TEXT,
  target_wake_time TEXT,
  duration_minutes INTEGER,
  wake_mood INTEGER,
  energy INTEGER,
  sleep_quality INTEGER,
  wakings_count INTEGER DEFAULT 0,
  sleep_onset TEXT DEFAULT 'normal', -- easy, normal, difficult
  wake_method TEXT DEFAULT 'on_time', -- before_630, on_time, slept_in
  rested_level INTEGER DEFAULT 3,
  bed_routine_completed INTEGER DEFAULT 0,
  wake_routine_completed INTEGER DEFAULT 0,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Exercise logs - Resistance training
CREATE TABLE IF NOT EXISTS exercise_resistance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  workout_id TEXT,
  workout_duration_min INTEGER,
  exercise_type TEXT, -- pushups, pullups, squats, etc
  reps INTEGER,
  sets INTEGER,
  intensity INTEGER, -- 1-5
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Exercise logs - Cardio
CREATE TABLE IF NOT EXISTS exercise_cardio (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  type TEXT, -- run, cycle, swim, hiit
  duration_min INTEGER,
  distance_km REAL,
  avg_pace REAL,
  is_intense INTEGER DEFAULT 0, -- 0 = normal, 1 = HIIT/intense
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS weight_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    weight_kg REAL NOT NULL,
    notes TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS daily_health_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    steps INTEGER,
    calories INTEGER,
    water_liters REAL,
    high_protein INTEGER DEFAULT 0,
    high_carbs INTEGER DEFAULT 0,
    high_fat INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_daily_health_date ON daily_health_logs(date);

-- Journaling
CREATE TABLE IF NOT EXISTS journal_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL UNIQUE,
  content TEXT DEFAULT '',
  submitted INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_journal_entries_date ON journal_entries(date);

-- Projects
CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  category TEXT, -- PhD, Work, Personal
  priority INTEGER, -- 1-3 (keeping for backward compatibility)
  tier INTEGER DEFAULT 4, -- 1-4: 1=ASAP, 2=High Priority, 3=Medium Priority, 4=Low Priority
  importance INTEGER, -- 1-3 for XP calculation
  status TEXT, -- active, paused, completed, archived
  thesis TEXT,
  last_touched TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Project sessions
CREATE TABLE IF NOT EXISTS project_sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER,
  date TEXT NOT NULL,
  duration_min INTEGER,
  micro_output TEXT,
  completed INTEGER DEFAULT 0, -- 0 = session, 1 = project completed
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(project_id) REFERENCES projects(id)
);

-- Tasks (separate from projects - simpler to-do items)
CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  text TEXT NOT NULL,
  completed INTEGER DEFAULT 0,
  order_index INTEGER NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Finance tracking
CREATE TABLE IF NOT EXISTS finance_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  cash REAL,
  investments REAL,
  debt REAL,
  net_worth REAL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS finance_fortnights (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  income REAL,
  fixed_costs REAL,
  variable_spend REAL,
  savings REAL,
  budget_met INTEGER DEFAULT 0, -- 0 = no, 1 = yes
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS income_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fortnight_id INTEGER NOT NULL,
    source_name TEXT NOT NULL,
    amount REAL NOT NULL,
    FOREIGN KEY (fortnight_id) REFERENCES finance_fortnights(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS weekly_finance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  week_start_date TEXT NOT NULL,
  week_end_date TEXT NOT NULL,
  income REAL NOT NULL,
  essentials REAL NOT NULL,
  discretionary REAL NOT NULL,
  savings REAL NOT NULL,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(week_start_date, week_end_date)
);

CREATE TABLE IF NOT EXISTS finance_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  decision TEXT,
  expected_outcome TEXT,
  confidence INTEGER, -- 1-5
  result TEXT,
  outcome_logged INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT NOT NULL, -- stocks, property, vehicle, cash, retirement, other
    current_value REAL NOT NULL,
    date_updated TEXT NOT NULL,
    notes TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS liabilities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT NOT NULL, -- mortgage, loan, credit_card, student_loan, other
    current_value REAL NOT NULL,
    date_updated TEXT NOT NULL,
    notes TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS routine_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    time_available TEXT NOT NULL,
    bonus_xp INTEGER NOT NULL,
    active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS routine_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL,
    item_text TEXT NOT NULL,
    xp_value INTEGER NOT NULL,
    parent_item_id INTEGER DEFAULT NULL,
    order_index INTEGER NOT NULL,
    active INTEGER DEFAULT 1,
    FOREIGN KEY (template_id) REFERENCES routine_templates(id),
    FOREIGN KEY (parent_item_id) REFERENCES routine_items(id)
);

CREATE TABLE IF NOT EXISTS daily_routine_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    completed INTEGER DEFAULT 0,
    FOREIGN KEY (template_id) REFERENCES routine_templates(id),
    FOREIGN KEY (item_id) REFERENCES routine_items(id),
    UNIQUE(date, template_id, item_id)
);

CREATE TABLE IF NOT EXISTS routine_submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    submitted_at TEXT NOT NULL,
    all_completed INTEGER NOT NULL,
    total_xp INTEGER NOT NULL,
    FOREIGN KEY (template_id) REFERENCES routine_templates(id),
    UNIQUE(date, template_id)
);

-- Experiments (trials)
CREATE TABLE IF NOT EXISTS experiments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  hypothesis TEXT,
  metric TEXT, -- what you're measuring
  baseline_start TEXT,
  baseline_end TEXT,
  baseline_value REAL,
  intervention_start TEXT,
  intervention_end TEXT,
  intervention_value REAL,
  conclusion TEXT,
  status TEXT, -- planning, baseline, intervention, complete
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Energy ROI tracking
CREATE TABLE IF NOT EXISTS energy_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  datetime TEXT NOT NULL,
  activity TEXT,
  energy_before INTEGER, -- 1-5
  energy_after INTEGER, -- 1-5
  duration_min INTEGER,
  context TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Identity levels
CREATE TABLE IF NOT EXISTS identity_levels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  domain TEXT NOT NULL, -- sleep, exercise, projects, finance
  level INTEGER DEFAULT 1, -- 1-5
  level_name TEXT, -- "Novice Sleeper", etc
  xp INTEGER DEFAULT 0,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- XP logs (for tracking XP gains)
CREATE TABLE IF NOT EXISTS xp_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  domain TEXT,
  activity TEXT,
  xp_gained INTEGER,
  multiplier REAL DEFAULT 1.0,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Daily quests
CREATE TABLE IF NOT EXISTS daily_quests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  quest_text TEXT,
  quest_type TEXT, -- sleep, exercise, projects, finance, cross_domain
  difficulty TEXT, -- easy, medium, hard
  xp_reward INTEGER,
  completed INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Reflections
CREATE TABLE IF NOT EXISTS reflections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  type TEXT, -- weekly, monthly
  what_went_well TEXT,
  what_drained_energy TEXT,
  what_gave_energy TEXT,
  patterns_noticed TEXT,
  small_change TEXT,
  overall_rating INTEGER, -- 1-10
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Affirmations/statements shown
CREATE TABLE IF NOT EXISTS affirmation_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  affirmation_text TEXT,
  affirmation_type TEXT, -- identity, data_insight, encouragement
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Pattern discoveries
CREATE TABLE IF NOT EXISTS discovered_patterns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  discovery_date TEXT NOT NULL,
  pattern_type TEXT, -- correlation, trend, personal_best
  description TEXT,
  metric1 TEXT,
  metric2 TEXT,
  correlation_value REAL,
  significance TEXT, -- weak, moderate, strong
  shown_to_user INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- App settings/config
CREATE TABLE IF NOT EXISTS app_config (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Reading / writing / debate / race / exam tracking (Renaissance trees)
CREATE TABLE IF NOT EXISTS books (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date_finished TEXT NOT NULL,
  title TEXT NOT NULL,
  category TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS essays (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date_published TEXT NOT NULL,
  title TEXT,
  url TEXT,
  word_count INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS debate_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  event_type TEXT,
  result TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS races (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  race_type TEXT NOT NULL, -- 5k|10k|half|marathon
  official INTEGER NOT NULL DEFAULT 0,
  time_seconds INTEGER,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exams (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exam_id TEXT NOT NULL,
  exam_group TEXT,
  date TEXT NOT NULL,
  passed INTEGER NOT NULL DEFAULT 0,
  score REAL,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Quarterly execution cycles
CREATE TABLE IF NOT EXISTS quarterly_cycles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  year INTEGER NOT NULL,
  quarter TEXT NOT NULL, -- Q1, Q2, Q3, Q4
  title TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  execution_end_date TEXT NOT NULL,
  summary_note TEXT DEFAULT '',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(year, quarter)
);

CREATE TABLE IF NOT EXISTS cycle_goals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cycle_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  description TEXT DEFAULT '',
  goal_type TEXT NOT NULL, -- counter, binary_recurring, milestone, measured
  display_order INTEGER NOT NULL DEFAULT 0,
  is_archived INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', -- active, complete, paused, dropped
  target_value REAL,
  current_value REAL,
  unit TEXT DEFAULT '',
  baseline_value REAL,
  target_direction TEXT, -- increase, decrease
  color_tag TEXT,
  notes TEXT DEFAULT '',
  carry_forward_default INTEGER DEFAULT 0,
  category TEXT DEFAULT '',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(cycle_id) REFERENCES quarterly_cycles(id)
);

CREATE TABLE IF NOT EXISTS counter_goal_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  goal_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  delta REAL NOT NULL,
  new_value REAL NOT NULL,
  note TEXT DEFAULT '',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(goal_id) REFERENCES cycle_goals(id)
);

CREATE TABLE IF NOT EXISTS recurring_goal_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  goal_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  value INTEGER NOT NULL, -- 1 true, 0 false
  note TEXT DEFAULT '',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(goal_id, date),
  FOREIGN KEY(goal_id) REFERENCES cycle_goals(id)
);

CREATE TABLE IF NOT EXISTS milestone_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  goal_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  description TEXT DEFAULT '',
  display_order INTEGER NOT NULL DEFAULT 0,
  is_completed INTEGER NOT NULL DEFAULT 0,
  completed_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(goal_id) REFERENCES cycle_goals(id)
);

CREATE TABLE IF NOT EXISTS measured_goal_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  goal_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  value REAL NOT NULL,
  note TEXT DEFAULT '',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(goal_id) REFERENCES cycle_goals(id)
);

CREATE TABLE IF NOT EXISTS cycle_notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cycle_id INTEGER NOT NULL,
  note_type TEXT NOT NULL, -- review, reflection, weekly
  content TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(cycle_id) REFERENCES quarterly_cycles(id)
);

CREATE INDEX IF NOT EXISTS idx_cycle_goals_cycle ON cycle_goals(cycle_id, display_order);
CREATE INDEX IF NOT EXISTS idx_counter_goal_events_goal_date ON counter_goal_events(goal_id, date);
CREATE INDEX IF NOT EXISTS idx_recurring_goal_logs_goal_date ON recurring_goal_logs(goal_id, date);
CREATE INDEX IF NOT EXISTS idx_milestone_items_goal_order ON milestone_items(goal_id, display_order);
CREATE INDEX IF NOT EXISTS idx_measured_goal_entries_goal_date ON measured_goal_entries(goal_id, date);

-- Initialize default identity levels
INSERT OR IGNORE INTO identity_levels (domain, level, level_name, xp) VALUES
  ('sleep', 1, 'Novice Sleeper', 0),
  ('health', 1, 'Beginner', 0),
  ('projects', 1, 'Dabbler', 0),
  ('finance', 1, 'Awareness', 0);

-- Initialize default config
INSERT OR IGNORE INTO app_config (key, value) VALUES
  ('target_bedtime', '22:30'),
  ('target_wake_time', '06:30'),
  ('weekly_exercise_goal_minutes', '150'),
  ('fortnight_budget', '1000'),
  ('current_season', '1'),
  ('season_start_date', date('now'));

-- Initialize default routines
INSERT OR IGNORE INTO routine_templates (id, name, time_available, bonus_xp, active) VALUES
  (1, 'Morning', '00:00', 50, 1),
  (2, 'Evening', '16:00', 30, 1),
  (3, 'Bedtime', '20:30', 100, 1);

-- Morning routine items
INSERT OR IGNORE INTO routine_items (id, template_id, item_text, xp_value, parent_item_id, order_index, active) VALUES
  (1, 1, 'Shower', 5, NULL, 1, 1),
  (2, 1, 'Brush Teeth', 5, NULL, 2, 1),
  (3, 1, 'Breakfast', 5, NULL, 3, 1),
  (4, 1, 'Coffee', 5, NULL, 4, 1),
  (5, 1, 'Deodorant', 5, NULL, 5, 1),
  (6, 1, 'Bag Packed', 5, NULL, 6, 1),
  (7, 1, 'Charging cable', 2, 6, 7, 1),
  (8, 1, 'Book', 2, 6, 8, 1),
  (9, 1, 'Laptop', 2, 6, 9, 1),
  (10, 1, 'Lunch', 2, 6, 10, 1),
  (11, 1, 'Water bottle', 2, 6, 11, 1);

INSERT INTO routine_items (template_id, item_text, xp_value, parent_item_id, order_index, active)
SELECT 1, 'Completed sleep log', 5, NULL, 12, 1
WHERE NOT EXISTS (
  SELECT 1
  FROM routine_items
  WHERE template_id = 1 AND item_text = 'Completed sleep log' AND active = 1
);

-- Evening routine items
INSERT OR IGNORE INTO routine_items (id, template_id, item_text, xp_value, parent_item_id, order_index, active) VALUES
  (12, 2, 'Lunch packed for tomorrow', 5, NULL, 1, 1),
  (13, 2, 'Clothes packed for tomorrow', 5, NULL, 2, 1),
  (14, 2, 'Check calendar', 5, NULL, 3, 1);

-- Bedtime routine items  
INSERT OR IGNORE INTO routine_items (id, template_id, item_text, xp_value, parent_item_id, order_index, active) VALUES
  (15, 3, 'Shower', 5, NULL, 1, 1),
  (16, 3, 'Brush Teeth', 5, NULL, 2, 1),
  (17, 3, 'Stretch', 5, NULL, 3, 1),
  (18, 3, 'Journal', 5, NULL, 4, 1),
  (19, 3, 'No money spent outside budget', 5, NULL, 5, 1),
  (20, 3, 'No masturbation', 5, NULL, 6, 1);
