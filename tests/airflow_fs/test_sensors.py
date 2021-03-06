import posixpath

from airflow_fs import sensors
from airflow_fs.hooks import LocalHook


class TestFileSensor:
    """Tests for the FileSensor class."""

    def test_files_present(self, local_mock_dir, test_dag):
        """Tests example with present files."""

        task = sensors.FileSensor(
            path=posixpath.join(local_mock_dir, "*.txt"),
            hook=LocalHook(),
            task_id="file_sensor",
            dag=test_dag
        )
        assert task.poke({})

    def test_files_missing(self, local_mock_dir, test_dag):
        """Tests example with missing files."""

        task = sensors.FileSensor(
            path=posixpath.join(local_mock_dir, "*.xml"),
            hook=LocalHook(),
            task_id="file_sensor",
            dag=test_dag
        )
        assert not task.poke({})

    def test_non_existing_dir(self, local_mock_dir, test_dag):
        """Tests example with non-existing dir."""

        task = sensors.FileSensor(
            path=posixpath.join(local_mock_dir, "non_existing_dir", "*.xml"),
            hook=LocalHook(),
            task_id="file_sensor",
            dag=test_dag
        )
        assert not task.poke({})
