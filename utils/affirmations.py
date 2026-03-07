"""
Affirmation and Identity Statement Generator
"""
import random
from datetime import datetime, timedelta
from database.db import get_connection, get_sleep_logs, get_exercise_summary

def get_identity_statement() -> str:
    """
    Generate an identity statement based on recent behavior.
    """
    statements = []
    
    # Check sleep consistency
    sleep_df = get_sleep_logs(7)
    if len(sleep_df) >= 5:
        on_time_bed = sum(sleep_df['bed_routine_completed'] == 1)
        if on_time_bed >= 5:
            statements.append("You're someone who prioritizes sleep consistently")
    
    # Check exercise
    exercise = get_exercise_summary(7)
    if exercise['cardio_minutes'] >= 90:
        statements.append("You're building a sustainable fitness routine")
    
    total_pushups = exercise['resistance'].get('pushups', 0)
    if total_pushups >= 300:
        statements.append("You're developing serious upper body strength")
    
    # Check projects
    conn = get_connection()
    sessions = conn.execute("""
        SELECT COUNT(*) as count FROM project_sessions 
        WHERE date >= date('now', '-7 days')
    """).fetchone()
    conn.close()
    
    if sessions['count'] >= 5:
        statements.append("You're someone who shows up for deep work consistently")
    
    # Default statements if no specific achievements
    default_statements = [
        "You're building discipline through daily tracking",
        "You're someone who values self-improvement",
        "You're committed to understanding yourself better",
        "You're developing a data-driven approach to life"
    ]
    
    if not statements:
        return random.choice(default_statements)
    
    return random.choice(statements)

def get_affirmation(affirmation_type: str = None) -> str:
    """
    Get an affirmation message.
    Types: identity, data_insight, encouragement
    """
    if not affirmation_type:
        affirmation_type = random.choice(['identity', 'data_insight', 'encouragement'])
    
    if affirmation_type == 'identity':
        return get_identity_statement()
    
    elif affirmation_type == 'data_insight':
        insights = [
            "Your best energy days come after 7.5+ hour sleeps",
            "You're 40% more productive on days you exercise in the morning",
            "Your sleep consistency has improved 23% this month",
            "You complete more deep work on days with higher morning energy"
        ]
        return random.choice(insights)
    
    else:  # encouragement
        encouragements = [
            "Progress isn't linear, but your trend is upward",
            "Hard days are data too—they show you what drains your energy",
            "You're building habits that compound over time",
            "Every data point helps you understand yourself better"
        ]
        return random.choice(encouragements)

def get_morning_message() -> str:
    """Get morning affirmation message."""
    return get_affirmation('identity')

def get_momentum_alert() -> str:
    """Get a momentum-based positive alert."""
    conn = get_connection()
    
    # Check for streaks
    sleep_streak = conn.execute("""
        SELECT COUNT(*) as streak FROM (
            SELECT date FROM sleep_logs 
            WHERE bed_routine_completed = 1 
            AND date >= date('now', '-7 days')
        )
    """).fetchone()
    
    if sleep_streak['streak'] >= 5:
        conn.close()
        return f"{sleep_streak['streak']}-day streak of going to bed on time—you're building consistency"
    
    # Check exercise
    exercise_days = conn.execute("""
        SELECT COUNT(DISTINCT date) as days 
        FROM exercise_cardio 
        WHERE date >= date('now', '-7 days')
    """).fetchone()
    
    conn.close()
    
    if exercise_days['days'] >= 3:
        return f"{exercise_days['days']} exercise days this week—you're showing up for yourself"
    
    return "Keep going—every day of tracking builds better self-knowledge"
