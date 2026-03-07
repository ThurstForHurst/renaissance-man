"""
XP Scoring and Identity Level Management
"""
from database.db import log_xp, get_identity_levels, update_identity_level

DOMAIN_XP_PER_LEVEL = 500

def calculate_sleep_xp(bedtime_on_time: bool, wake_on_time: bool, 
                       bed_routine: bool = False, wake_routine: bool = False) -> int:
    """
    Calculate XP for sleep:
    - Bed on time: 50 XP
    - Wake on time: 50 XP
    - Bed routine completed: +25 XP
    - Wake routine completed: +25 XP
    """
    xp = 0
    if bedtime_on_time:
        xp += 50
    if wake_on_time:
        xp += 50
    if bed_routine:
        xp += 25
    if wake_routine:
        xp += 25
    return xp

def calculate_resistance_xp(exercise_type: str, reps: int, sets: int = 1, intensity: int = 3) -> int:
    """
    Calculate XP for resistance training:
    - Push-ups: 1 XP per rep
    - Pull-ups: 2 XP per rep
    - Squats: 0.5 XP per rep
    - Intensity ≥4: 1.5x multiplier
    """
    xp_per_rep = {
        'pushups': 1,
        'pullups': 2,
        'squats': 0.5,
        'situps': 0.5,
        'dips': 1.5,
        'lunges': 0.5
    }
    
    base_xp = xp_per_rep.get(exercise_type.lower(), 1) * reps * sets
    
    # Intensity multiplier
    if intensity >= 4:
        base_xp *= 1.5
    
    return int(base_xp)

def calculate_cardio_xp(duration_min: int, is_intense: bool = False, distance_km: float = None) -> int:
    """
    Calculate XP for cardio:
    - Normal intensity: 2 XP per minute
    - Intense (HIIT): 5 XP per minute
    - Distance bonus: +10 XP per km
    """
    if is_intense:
        xp = duration_min * 5
    else:
        xp = duration_min * 2
    
    # Distance bonus
    if distance_km:
        xp += int(distance_km * 10)
    
    return xp

def calculate_project_xp(duration_min: int, importance: int = 2, completed: bool = False) -> int:
    """
    Calculate XP for project work:
    - 1 XP per minute of work
    - Completion bonus: duration × importance × 0.5
    - Micro-output documented: +20 XP (handled separately)
    """
    base_xp = duration_min
    
    if completed:
        completion_bonus = int(duration_min * importance * 0.5)
        base_xp += completion_bonus
    
    return base_xp

def calculate_finance_xp(savings: float) -> int:
    """Calculate XP for finance tracking based on savings amount (budget_met removed)."""
    base_xp = 50  # Base XP for logging a fortnight
    
    # Additional XP based on savings amount
    if savings >= 2000:
        savings_xp = 100
    elif savings >= 1000:
        savings_xp = 75
    elif savings >= 500:
        savings_xp = 50
    elif savings >= 0:
        savings_xp = 25
    else:
        savings_xp = 0  # No extra XP for negative savings
    
    return base_xp + savings_xp

def award_xp(date: str, domain: str, activity: str, xp: int, multiplier: float = 1.0, notes: str = ""):
    """
    Award XP and check for level-ups.
    """
    final_xp = log_xp(date, domain, activity, xp, multiplier, notes)
    
    # Check for level-up
    check_level_up(domain)
    
    return final_xp

def check_level_up(domain: str):
    """
    Check if a domain has leveled up and update if so.
    """
    levels = get_identity_levels()
    current = next((l for l in levels if l['domain'] == domain), None)
    
    if not current:
        return
    
    current_xp = current['xp']
    current_level = current['level']
    
    # Linear infinite level system: every 500 XP is one level.
    new_level = (current_xp // DOMAIN_XP_PER_LEVEL) + 1
    new_name = f"Level {new_level}"
    
    # Update if leveled up
    if new_level != current_level or current.get('level_name') != new_name:
        update_identity_level(domain, new_level, new_name)
        return new_level > current_level
    
    return False

def get_level_progress(domain: str) -> dict:
    """
    Get progress toward next level.
    """
    levels = get_identity_levels()
    current = next((l for l in levels if l['domain'] == domain), None)
    
    if not current:
        return None
    
    current_xp = current['xp']
    current_level = (current_xp // DOMAIN_XP_PER_LEVEL) + 1
    progress = current_xp % DOMAIN_XP_PER_LEVEL
    needed = DOMAIN_XP_PER_LEVEL
    percentage = (progress / needed) * 100
    
    return {
        'current_level': current_level,
        'current_name': f"Level {current_level}",
        'current_xp': current_xp,
        'next_level': current_level + 1,
        'next_name': f"Level {current_level + 1}",
        'next_threshold': needed,
        'progress': progress,
        'needed': needed,
        'percentage': round(percentage, 1),
        'xp_per_level': DOMAIN_XP_PER_LEVEL,
        'max_level': False
    }

def get_all_progress() -> dict:
    """
    Get level progress for all domains.
    """
    return {
        'sleep': get_level_progress('sleep'),
        'health': get_level_progress('health'),
        'projects': get_level_progress('projects'),
        'finance': get_level_progress('finance')
    }
