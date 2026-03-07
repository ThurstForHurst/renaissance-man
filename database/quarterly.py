"""Quarterly cycle service layer with phase-enforced mutations."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from database.db import get_connection, get_brisbane_date

GOAL_TYPES = {"counter", "binary_recurring", "milestone", "measured"}
GOAL_STATUSES = {"active", "complete", "paused", "dropped"}
TARGET_DIRECTIONS = {"increase", "decrease"}


def _today() -> date:
    return date.fromisoformat(get_brisbane_date())


def _quarter_meta_for_date(on_date: date) -> Tuple[int, str, date, date]:
    if on_date.month <= 3:
        q, start_m, end_m, end_d = "Q1", 1, 3, 31
    elif on_date.month <= 6:
        q, start_m, end_m, end_d = "Q2", 4, 6, 30
    elif on_date.month <= 9:
        q, start_m, end_m, end_d = "Q3", 7, 9, 30
    else:
        q, start_m, end_m, end_d = "Q4", 10, 12, 31
    year = on_date.year
    return year, q, date(year, start_m, 1), date(year, end_m, end_d)


def _phase_for(cycle: Dict[str, Any], on_date: Optional[date] = None) -> str:
    now = on_date or _today()
    execution_end = date.fromisoformat(cycle["execution_end_date"])
    end_date = date.fromisoformat(cycle["end_date"])
    # execution_end_date is treated as the first day of review, not the last day of execution.
    if now < execution_end:
        return "execution"
    if now <= end_date:
        return "review"
    return "complete"


def _cycle_progress(cycle: Dict[str, Any], on_date: Optional[date] = None) -> Dict[str, float]:
    now = on_date or _today()
    start_date = date.fromisoformat(cycle["start_date"])
    end_date = date.fromisoformat(cycle["end_date"])
    execution_end = date.fromisoformat(cycle["execution_end_date"])
    total_days = max(1, (end_date - start_date).days)
    elapsed = (now - start_date).days
    pct = max(0.0, min(100.0, (elapsed / total_days) * 100.0))

    execution_total = max(1, (execution_end - start_date).days)
    execution_elapsed = min(max((now - start_date).days, 0), execution_total)
    execution_pct = max(0.0, min(100.0, (execution_elapsed / execution_total) * 100.0))

    # Human-readable week position across the full quarter.
    total_weeks = max(1, ((end_date - start_date).days + 1 + 6) // 7)
    current_week = max(1, min(total_weeks, ((max(0, elapsed)) // 7) + 1))

    # Execution segment width as a percentage of the full cycle bar.
    execution_span_pct = max(0.0, min(100.0, (execution_total / total_days) * 100.0))

    return {
        "cycle_pct": pct,
        "execution_pct": execution_pct,
        "total_weeks": total_weeks,
        "current_week": current_week,
        "execution_span_pct": execution_span_pct,
    }


def _require_structural_edit(cycle_id: int, admin_override: bool = False):
    if admin_override:
        return
    snapshot = get_cycle_snapshot(cycle_id)
    if snapshot["cycle"]["phase"] != "review":
        raise ValueError("Structural editing is locked during execution phase.")


def _to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def get_or_create_current_cycle() -> Dict[str, Any]:
    now = _today()
    year, quarter, start_date, end_date = _quarter_meta_for_date(now)
    execution_end = start_date + timedelta(days=69)
    title = f"{quarter} {year}"

    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM quarterly_cycles WHERE year = ? AND quarter = ?",
        (year, quarter),
    ).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO quarterly_cycles
            (year, quarter, title, start_date, end_date, execution_end_date)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (year, quarter, title, start_date.isoformat(), end_date.isoformat(), execution_end.isoformat()),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM quarterly_cycles WHERE year = ? AND quarter = ?",
            (year, quarter),
        ).fetchone()
    conn.close()
    return dict(row)


def get_cycles_history() -> List[Dict[str, Any]]:
    get_or_create_current_cycle()
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT c.*,
               (SELECT COUNT(*) FROM cycle_goals g WHERE g.cycle_id = c.id AND g.is_archived = 0) AS active_goals
        FROM quarterly_cycles c
        ORDER BY c.year DESC, c.quarter DESC
        """
    ).fetchall()
    conn.close()
    cycles = _to_dicts(rows)
    for c in cycles:
        c["phase"] = _phase_for(c)
    return cycles


def _calc_binary_metrics(goal_id: int, cycle: Dict[str, Any], conn) -> Dict[str, Any]:
    start_date = date.fromisoformat(cycle["start_date"])
    end_date = date.fromisoformat(cycle["end_date"])
    today = _today()
    rows = conn.execute(
        """
        SELECT date, value
        FROM recurring_goal_logs
        WHERE goal_id = ? AND date BETWEEN ? AND ?
        ORDER BY date ASC
        """,
        (goal_id, start_date.isoformat(), end_date.isoformat()),
    ).fetchall()
    logs = [dict(r) for r in rows]
    cycle_days = max(1, (min(today, end_date) - start_date).days + 1)
    success_count = sum(1 for r in logs if int(r["value"]) == 1)
    cycle_compliance = (success_count / cycle_days) * 100.0

    week_start = today - timedelta(days=6)
    weekly = [r for r in logs if week_start.isoformat() <= r["date"] <= today.isoformat()]
    week_success = sum(1 for r in weekly if int(r["value"]) == 1)
    week_compliance = (week_success / 7.0) * 100.0

    streak = 0
    cursor = today
    status_by_date = {r["date"]: int(r["value"]) for r in logs}
    while cursor >= start_date:
        v = status_by_date.get(cursor.isoformat())
        if v != 1:
            break
        streak += 1
        cursor -= timedelta(days=1)

    return {
        "logs": logs,
        "current_streak": streak,
        "week_compliance_pct": max(0.0, min(100.0, week_compliance)),
        "cycle_compliance_pct": max(0.0, min(100.0, cycle_compliance)),
        "today_status": status_by_date.get(today.isoformat()),
    }


def _calc_measured_progress(goal: Dict[str, Any], conn) -> Dict[str, Any]:
    entries = _to_dicts(
        conn.execute(
            "SELECT * FROM measured_goal_entries WHERE goal_id = ? ORDER BY date DESC, id DESC",
            (goal["id"],),
        ).fetchall()
    )
    baseline = float(goal["baseline_value"]) if goal["baseline_value"] is not None else None
    target = float(goal["target_value"]) if goal["target_value"] is not None else None
    current = None
    if entries:
        current = float(entries[0]["value"])
    elif goal["current_value"] is not None:
        current = float(goal["current_value"])

    progress_pct = 0.0
    net_change = None
    if baseline is not None and target is not None and current is not None and baseline != target:
        total_gap = abs(target - baseline)
        if (goal.get("target_direction") or "increase") == "decrease":
            advanced = baseline - current
        else:
            advanced = current - baseline
        progress_pct = (advanced / total_gap) * 100.0
        net_change = current - baseline

    return {
        "entries": entries[:20],
        "current": current,
        "progress_pct": max(0.0, min(100.0, progress_pct)),
        "net_change": net_change,
    }


def _goal_with_derived(goal: Dict[str, Any], cycle: Dict[str, Any], conn) -> Dict[str, Any]:
    g = dict(goal)
    g["derived"] = {}
    goal_type = g["goal_type"]

    if goal_type == "counter":
        target = float(g["target_value"]) if g["target_value"] is not None else 0.0
        current = float(g["current_value"]) if g["current_value"] is not None else 0.0
        g["derived"]["progress_pct"] = max(0.0, min(100.0, (current / target * 100.0) if target > 0 else 0.0))
        g["derived"]["events"] = _to_dicts(
            conn.execute(
                "SELECT * FROM counter_goal_events WHERE goal_id = ? ORDER BY date DESC, id DESC LIMIT 12",
                (g["id"],),
            ).fetchall()
        )
    elif goal_type == "binary_recurring":
        g["derived"] = _calc_binary_metrics(g["id"], cycle, conn)
    elif goal_type == "milestone":
        milestones = _to_dicts(
            conn.execute(
                "SELECT * FROM milestone_items WHERE goal_id = ? ORDER BY display_order, id",
                (g["id"],),
            ).fetchall()
        )
        completed = sum(1 for m in milestones if int(m["is_completed"]) == 1)
        total = len(milestones)
        g["derived"]["milestones"] = milestones
        g["derived"]["completed"] = completed
        g["derived"]["total"] = total
        g["derived"]["progress_pct"] = (completed / total * 100.0) if total else 0.0
    elif goal_type == "measured":
        g["derived"] = _calc_measured_progress(g, conn)
    return g


def get_cycle_snapshot(cycle_id: Optional[int] = None) -> Dict[str, Any]:
    current_cycle = get_or_create_current_cycle()
    chosen_cycle_id = cycle_id or current_cycle["id"]

    conn = get_connection()
    cycle_row = conn.execute("SELECT * FROM quarterly_cycles WHERE id = ?", (chosen_cycle_id,)).fetchone()
    if not cycle_row:
        conn.close()
        raise ValueError("Cycle not found.")
    cycle = dict(cycle_row)
    phase = _phase_for(cycle)
    cycle["phase"] = phase
    cycle["is_current"] = int(cycle["id"]) == int(current_cycle["id"])
    cycle["is_read_only"] = phase == "complete" or not cycle["is_current"]
    cycle["can_structural_edit"] = phase == "review" and cycle["is_current"]
    cycle["can_progress_edit"] = phase in ("execution", "review") and cycle["is_current"]

    progress = _cycle_progress(cycle)
    cycle.update(progress)

    goals_rows = conn.execute(
        """
        SELECT *
        FROM cycle_goals
        WHERE cycle_id = ?
        ORDER BY is_archived ASC, display_order ASC, id ASC
        """,
        (chosen_cycle_id,),
    ).fetchall()
    goals = [_goal_with_derived(dict(r), cycle, conn) for r in goals_rows]

    notes = _to_dicts(
        conn.execute(
            "SELECT * FROM cycle_notes WHERE cycle_id = ? ORDER BY created_at DESC, id DESC",
            (chosen_cycle_id,),
        ).fetchall()
    )
    conn.close()

    return {
        "cycle": cycle,
        "goals": goals,
        "notes": notes,
    }


def add_cycle_goal(
    cycle_id: int,
    title: str,
    goal_type: str,
    description: str = "",
    target_value: Optional[float] = None,
    current_value: Optional[float] = None,
    unit: str = "",
    baseline_value: Optional[float] = None,
    target_direction: Optional[str] = None,
    category: str = "",
    notes: str = "",
    milestones: Optional[List[str]] = None,
    admin_override: bool = False,
) -> int:
    _require_structural_edit(cycle_id, admin_override)
    gtype = (goal_type or "").strip().lower()
    if gtype not in GOAL_TYPES:
        raise ValueError("Invalid goal type.")
    if not (title or "").strip():
        raise ValueError("Title is required.")
    if gtype == "counter" and target_value is None:
        raise ValueError("Counter goals require a target value.")
    if gtype == "measured":
        if baseline_value is None or target_value is None:
            raise ValueError("Measured goals require baseline and target.")
        if (target_direction or "").lower() not in TARGET_DIRECTIONS:
            raise ValueError("Measured goals require target direction.")
    if gtype == "milestone" and not (milestones or []):
        raise ValueError("Milestone goals require at least one milestone.")

    conn = get_connection()
    max_order_row = conn.execute(
        "SELECT COALESCE(MAX(display_order), 0) AS max_order FROM cycle_goals WHERE cycle_id = ?",
        (cycle_id,),
    ).fetchone()
    display_order = int(max_order_row["max_order"]) + 1
    cur = conn.execute(
        """
        INSERT INTO cycle_goals
        (cycle_id, title, description, goal_type, display_order, status, target_value, current_value, unit,
         baseline_value, target_direction, category, notes)
        VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cycle_id,
            title.strip(),
            (description or "").strip(),
            gtype,
            display_order,
            target_value,
            current_value,
            (unit or "").strip(),
            baseline_value,
            (target_direction or "").strip().lower() or None,
            (category or "").strip(),
            (notes or "").strip(),
        ),
    )
    goal_id = int(cur.lastrowid)
    if gtype == "milestone":
        cleaned = [m.strip() for m in (milestones or []) if (m or "").strip()]
        for idx, m in enumerate(cleaned, start=1):
            conn.execute(
                """
                INSERT INTO milestone_items (goal_id, title, display_order)
                VALUES (?, ?, ?)
                """,
                (goal_id, m, idx),
            )
    conn.commit()
    conn.close()
    return goal_id


def archive_cycle_goal(goal_id: int, admin_override: bool = False):
    conn = get_connection()
    goal = conn.execute("SELECT id, cycle_id FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    conn.close()
    if not goal:
        raise ValueError("Goal not found.")
    _require_structural_edit(int(goal["cycle_id"]), admin_override)
    conn = get_connection()
    conn.execute("UPDATE cycle_goals SET is_archived = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (goal_id,))
    conn.commit()
    conn.close()


def move_goal(goal_id: int, direction: str, admin_override: bool = False):
    conn = get_connection()
    goal = conn.execute(
        "SELECT id, cycle_id, display_order FROM cycle_goals WHERE id = ? AND is_archived = 0",
        (goal_id,),
    ).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    cycle_id = int(goal["cycle_id"])
    _require_structural_edit(cycle_id, admin_override)
    dir_sign = -1 if direction == "up" else 1
    swap = conn.execute(
        """
        SELECT id, display_order
        FROM cycle_goals
        WHERE cycle_id = ? AND is_archived = 0 AND display_order = ?
        """,
        (cycle_id, int(goal["display_order"]) + dir_sign),
    ).fetchone()
    if swap:
        conn.execute("UPDATE cycle_goals SET display_order = ? WHERE id = ?", (swap["display_order"], goal_id))
        conn.execute("UPDATE cycle_goals SET display_order = ? WHERE id = ?", (goal["display_order"], swap["id"]))
        conn.commit()
    conn.close()


def set_goal_status(goal_id: int, status: str):
    cleaned = (status or "").strip().lower()
    if cleaned not in GOAL_STATUSES:
        raise ValueError("Invalid status.")
    conn = get_connection()
    conn.execute(
        "UPDATE cycle_goals SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (cleaned, goal_id),
    )
    conn.commit()
    conn.close()


def log_counter_delta(goal_id: int, delta: float, note: str = "", entry_date: Optional[str] = None):
    conn = get_connection()
    goal = conn.execute(
        "SELECT id, cycle_id, goal_type, COALESCE(current_value, 0) AS current_value FROM cycle_goals WHERE id = ?",
        (goal_id,),
    ).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "counter":
        conn.close()
        raise ValueError("Goal is not a counter goal.")
    snapshot = get_cycle_snapshot(int(goal["cycle_id"]))
    if not snapshot["cycle"]["can_progress_edit"]:
        conn.close()
        raise ValueError("Progress updates are locked for this cycle.")
    new_value = float(goal["current_value"]) + float(delta)
    event_date = entry_date or get_brisbane_date()
    conn.execute(
        "UPDATE cycle_goals SET current_value = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (new_value, goal_id),
    )
    conn.execute(
        """
        INSERT INTO counter_goal_events (goal_id, date, delta, new_value, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (goal_id, event_date, float(delta), new_value, (note or "").strip()),
    )
    conn.commit()
    conn.close()


def set_counter_value(goal_id: int, new_value: float, note: str = "", entry_date: Optional[str] = None):
    conn = get_connection()
    goal = conn.execute(
        "SELECT id, cycle_id, goal_type, COALESCE(current_value, 0) AS current_value FROM cycle_goals WHERE id = ?",
        (goal_id,),
    ).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "counter":
        conn.close()
        raise ValueError("Goal is not a counter goal.")
    snapshot = get_cycle_snapshot(int(goal["cycle_id"]))
    if not snapshot["cycle"]["can_progress_edit"]:
        conn.close()
        raise ValueError("Progress updates are locked for this cycle.")
    previous = float(goal["current_value"])
    next_value = float(new_value)
    delta = next_value - previous
    event_date = entry_date or get_brisbane_date()
    conn.execute(
        "UPDATE cycle_goals SET current_value = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (next_value, goal_id),
    )
    conn.execute(
        """
        INSERT INTO counter_goal_events (goal_id, date, delta, new_value, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (goal_id, event_date, delta, next_value, (note or "").strip()),
    )
    conn.commit()
    conn.close()


def log_recurring_value(goal_id: int, entry_date: str, value: bool, note: str = ""):
    conn = get_connection()
    goal = conn.execute(
        "SELECT id, cycle_id, goal_type FROM cycle_goals WHERE id = ?",
        (goal_id,),
    ).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "binary_recurring":
        conn.close()
        raise ValueError("Goal is not a binary recurring goal.")
    snapshot = get_cycle_snapshot(int(goal["cycle_id"]))
    if not snapshot["cycle"]["can_progress_edit"]:
        conn.close()
        raise ValueError("Progress updates are locked for this cycle.")
    conn.execute(
        """
        INSERT INTO recurring_goal_logs (goal_id, date, value, note)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(goal_id, date) DO UPDATE SET
            value = excluded.value,
            note = excluded.note
        """,
        (goal_id, entry_date, 1 if value else 0, (note or "").strip()),
    )
    conn.commit()
    conn.close()


def toggle_milestone_completion(goal_id: int, milestone_id: int, is_completed: bool):
    conn = get_connection()
    goal = conn.execute("SELECT id, cycle_id, goal_type FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "milestone":
        conn.close()
        raise ValueError("Goal is not a milestone goal.")
    snapshot = get_cycle_snapshot(int(goal["cycle_id"]))
    if not snapshot["cycle"]["can_progress_edit"]:
        conn.close()
        raise ValueError("Progress updates are locked for this cycle.")
    conn.execute(
        """
        UPDATE milestone_items
        SET is_completed = ?,
            completed_at = CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND goal_id = ?
        """,
        (1 if is_completed else 0, 1 if is_completed else 0, milestone_id, goal_id),
    )
    conn.commit()
    conn.close()


def add_milestone(goal_id: int, title: str, admin_override: bool = False):
    conn = get_connection()
    goal = conn.execute("SELECT id, cycle_id, goal_type FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "milestone":
        conn.close()
        raise ValueError("Goal is not a milestone goal.")
    _require_structural_edit(int(goal["cycle_id"]), admin_override)
    row = conn.execute(
        "SELECT COALESCE(MAX(display_order), 0) AS max_order FROM milestone_items WHERE goal_id = ?",
        (goal_id,),
    ).fetchone()
    conn.execute(
        "INSERT INTO milestone_items (goal_id, title, display_order) VALUES (?, ?, ?)",
        (goal_id, (title or "").strip(), int(row["max_order"]) + 1),
    )
    conn.commit()
    conn.close()


def rename_milestone(goal_id: int, milestone_id: int, title: str, admin_override: bool = False):
    conn = get_connection()
    goal = conn.execute("SELECT id, cycle_id, goal_type FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "milestone":
        conn.close()
        raise ValueError("Goal is not a milestone goal.")
    _require_structural_edit(int(goal["cycle_id"]), admin_override)
    conn.execute(
        "UPDATE milestone_items SET title = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND goal_id = ?",
        ((title or "").strip(), milestone_id, goal_id),
    )
    conn.commit()
    conn.close()


def delete_milestone(goal_id: int, milestone_id: int, admin_override: bool = False):
    conn = get_connection()
    goal = conn.execute("SELECT id, cycle_id, goal_type FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "milestone":
        conn.close()
        raise ValueError("Goal is not a milestone goal.")
    _require_structural_edit(int(goal["cycle_id"]), admin_override)
    conn.execute("DELETE FROM milestone_items WHERE id = ? AND goal_id = ?", (milestone_id, goal_id))
    conn.commit()
    conn.close()


def move_milestone(goal_id: int, milestone_id: int, direction: str, admin_override: bool = False):
    conn = get_connection()
    goal = conn.execute("SELECT id, cycle_id, goal_type FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "milestone":
        conn.close()
        raise ValueError("Goal is not a milestone goal.")
    _require_structural_edit(int(goal["cycle_id"]), admin_override)
    item = conn.execute(
        "SELECT id, display_order FROM milestone_items WHERE id = ? AND goal_id = ?",
        (milestone_id, goal_id),
    ).fetchone()
    if item:
        sign = -1 if direction == "up" else 1
        swap = conn.execute(
            "SELECT id, display_order FROM milestone_items WHERE goal_id = ? AND display_order = ?",
            (goal_id, int(item["display_order"]) + sign),
        ).fetchone()
        if swap:
            conn.execute("UPDATE milestone_items SET display_order = ? WHERE id = ?", (swap["display_order"], milestone_id))
            conn.execute("UPDATE milestone_items SET display_order = ? WHERE id = ?", (item["display_order"], swap["id"]))
            conn.commit()
    conn.close()


def log_measured_value(goal_id: int, value: float, entry_date: Optional[str] = None, note: str = ""):
    conn = get_connection()
    goal = conn.execute(
        "SELECT id, cycle_id, goal_type FROM cycle_goals WHERE id = ?",
        (goal_id,),
    ).fetchone()
    if not goal:
        conn.close()
        raise ValueError("Goal not found.")
    if goal["goal_type"] != "measured":
        conn.close()
        raise ValueError("Goal is not a measured goal.")
    snapshot = get_cycle_snapshot(int(goal["cycle_id"]))
    if not snapshot["cycle"]["can_progress_edit"]:
        conn.close()
        raise ValueError("Progress updates are locked for this cycle.")
    log_date = entry_date or get_brisbane_date()
    conn.execute(
        """
        INSERT INTO measured_goal_entries (goal_id, date, value, note)
        VALUES (?, ?, ?, ?)
        """,
        (goal_id, log_date, float(value), (note or "").strip()),
    )
    conn.execute(
        "UPDATE cycle_goals SET current_value = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (float(value), goal_id),
    )
    conn.commit()
    conn.close()


def save_cycle_note(cycle_id: int, content: str, note_type: str = "review"):
    cleaned = (content or "").strip()
    if not cleaned:
        raise ValueError("Note cannot be empty.")
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO cycle_notes (cycle_id, note_type, content)
        VALUES (?, ?, ?)
        """,
        (cycle_id, (note_type or "review").strip(), cleaned),
    )
    conn.commit()
    conn.close()


def set_cycle_summary_note(cycle_id: int, summary_note: str, admin_override: bool = False):
    _require_structural_edit(cycle_id, admin_override)
    conn = get_connection()
    conn.execute(
        "UPDATE quarterly_cycles SET summary_note = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        ((summary_note or "").strip(), cycle_id),
    )
    conn.commit()
    conn.close()
