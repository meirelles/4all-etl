import unittest
from airflow.models import DagBag


class TestDagIntegrity(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )

    def test_import_time(self):
        threshold = self.LOAD_SECOND_THRESHOLD
        stats = self.dagbag.dagbag_stats
        slow_dags = filter(lambda d: d.duration > self.LOAD_SECOND_THRESHOLD, stats)
        res = ', '.join(map(lambda d: d.file[1:], slow_dags))
        self.assertEqual(
            0,
            len(list(slow_dags)),
            'The following files take more than {threshold}s to load: {res}'.format(threshold=threshold, res=res)
        )

