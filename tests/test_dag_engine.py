import os
import tempfile
import unittest

import database.db as db


class TestDagEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.tmp_dir.name, "dag_test.db")
        db.DB_PATH = cls.db_path
        db.init_db()

    @classmethod
    def tearDownClass(cls):
        cls.tmp_dir.cleanup()

    def test_cycle_detection(self):
        with self.assertRaises(ValueError):
            db.dag_add_edge("identity_architect", "data_cadence")

    def test_progress_math(self):
        self.assertAlmostEqual(db.compute_prereq_progress(5, ">=", 10), 0.5, places=3)
        self.assertAlmostEqual(db.compute_prereq_progress(12, ">=", 10), 1.0, places=3)
        self.assertAlmostEqual(db.compute_prereq_progress(5, "<=", 10), 1.0, places=3)
        self.assertLess(db.compute_prereq_progress(20, "<=", 10), 1.0)

    def test_availability_gating(self):
        db.dag_eval_all(db.DAG_DEFAULT_USER_ID)
        states = {s["node_id"]: s for s in db.get_dag_user_states(db.DAG_DEFAULT_USER_ID)}
        self.assertEqual(states["sleep_steward"]["state"], "locked")

        db.upsert_dag_user_node_state(
            "data_cadence",
            user_id=db.DAG_DEFAULT_USER_ID,
            state="unlocked",
            progress=1.0,
            near_miss=False,
        )
        db.dag_eval_all(db.DAG_DEFAULT_USER_ID)
        states = {s["node_id"]: s for s in db.get_dag_user_states(db.DAG_DEFAULT_USER_ID)}
        self.assertEqual(states["sleep_steward"]["state"], "available")


if __name__ == "__main__":
    unittest.main()
