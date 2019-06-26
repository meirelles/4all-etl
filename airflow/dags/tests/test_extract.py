import unittest
from operators.extract import GetDataset
from io import StringIO
import inspect


class TestGetDataset(unittest.TestCase):
    def test_parse(self):
        s = inspect.cleandoc("""
        Latitude: 30°03′21″S   -30.05596474
        Longitude: 51°10′22″W   -51.17286827
        Distance: 4.9213 km  Bearing: 118.814°
        """)

        self.assertDictEqual(
            GetDataset.next_geo_direction(StringIO(s)),
            dict(lat=-30.05596474, lng=-51.17286827, dist=4.9213, bearing=118.814)
        )

        s = inspect.cleandoc("""
        Latitude: 30°03′21″S   -30.05596474
        Distance: 4.9213 km  Bearing: 118.814°
        """)

        with self.assertRaises(ValueError):
            GetDataset.next_geo_direction(StringIO(s))

        s = "Latitude: 30°03′21″S   -30.05596474"

        with self.assertRaises(ValueError):
            GetDataset.next_geo_direction(StringIO(s))
