"""Module containing file system sensors."""

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from airflow_fs.hooks import LocalHook


class FileSensor(BaseSensorOperator):
    """Sensor that waits for files matching a given file pattern."""

    template_fields = ("file_pattern",)

    @apply_defaults
    def __init__(self, file_pattern, hook=None, **kwargs):
        super(FileSensor, self).__init__(**kwargs)

        self._file_pattern = file_pattern
        self._hook = hook or LocalHook()

    def poke(self, context):
        with self._hook as hook:
            if hook.glob(self._file_pattern):
                return True
            return False
