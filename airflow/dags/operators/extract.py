import re
import logging
import os
import fnmatch

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.utils import IntermediateWrite

log = logging.getLogger(__name__)


class GetDataset(BaseOperator):
    """
    Picks up the a file from file system location and store to a
    intermediary table.
    """

    template_fields = ('input_dir', 'input_mask', 'output_ns')

    @apply_defaults
    def __init__(self, input_dir, input_mask, output_ns, *args, **kwargs):
        """
        Create a new GetDataset instance.

        :param input_dir: input directory to lookup
        :param input_mask: input file mask
        :param output_ns: IntermediateWrite's namespace string
        """

        super(GetDataset, self).__init__(*args, **kwargs)
        self.input_dir = input_dir
        self.input_mask = input_mask
        self.output_ns = output_ns

    def execute(self, context):
        log.info("Searching for files like {0} in {1}".format(self.input_mask, self.input_dir))
        base_names = fnmatch.filter(os.listdir(self.input_dir), self.input_mask)
        iwrite = IntermediateWrite(self.output_ns)
        for base_name in base_names:
            self.process(iwrite, os.path.join(self.input_dir, base_name))
        iwrite.flush()

    def process(self, iwrite, filename):
        """
        Get input file records to output.

        :param iwrite: IntermediateWrite instance
        :param filename: the filename to process
        """

        log.info("Processing: " + filename)
        with open(filename, 'r') as handle:
            while True:
                try:
                    record = self.next_geo_direction(handle)
                except ValueError as e:
                    log.error(e)
                    continue
                if not record:
                    break
                iwrite.add(record)

    @staticmethod
    def next_geo_direction(handle):
        """
        Create a single dict record from input handle, sample:

        Latitude: 30°02′59″S   -30.04982864
        Longitude: 51°12′05″W   -51.20150245
        Distance: 2.2959 km  Bearing: 137.352°

        :param handle: file handle
        """

        def get_and_match(regex):
            line = handle.readline()
            if not line:
                raise ValueError("Missing record line")
            return match_only(regex, line)

        def match_only(regex, line):
            match = re.match(regex, line)
            if not match:
                raise ValueError("Bad line: " + line)
            return match

        peek_line = handle.readline()

        # It's ok, just EOF
        if not peek_line:
            return

        m = match_only(r"^Latitude: .*?\s+([0-9\.\+\-]+)$", peek_line)
        lat = float(m[1])

        m = get_and_match(r"^Longitude: .*?\s+([0-9\.\+\-]+)$")
        long = float(m[1])

        m = get_and_match(r"^Distance: ([0-9\.]+) km\s+Bearing: ([0-9\.\+\-]+)\S$")
        dist = float(m[1])
        bearing = float(m[2])

        return dict(lat=lat, lng=long, dist=dist, bearing=bearing)
