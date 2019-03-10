import datetime
import posixpath
import pytest

from airflow import DAG

from airflow_fs.hooks import S3Hook
from airflow_fs import operators


@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2018, 1, 1)},
        schedule_interval=datetime.timedelta(days=1),
    )


def _run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )


class TestCopyFileOperator:
    """Tests for the CopyFileOperator."""

    def test_single(self, s3_client, local_mock_dir, s3_temp_dir, test_dag):
        """Tests copying of single file."""

        dest_hook = S3Hook()
        dest_path = posixpath.join(s3_temp_dir, "test.txt")

        assert not dest_hook.exists(dest_path)

        task = operators.CopyFileOperator(
            src_path=posixpath.join(local_mock_dir, "test.txt"),
            dest_path=dest_path,
            dest_hook=dest_hook,
            task_id="copy_task",
            dag=test_dag
        )
        _run_task(task, test_dag)

        assert dest_hook.exists(dest_path)

    def test_glob(self, s3_client, local_mock_dir, s3_temp_dir, test_dag):
        """Tests copying of files using glob pattern."""

        dest_hook = S3Hook()

        task = operators.CopyFileOperator(
            src_path=posixpath.join(local_mock_dir, "*.csv"),
            dest_path=s3_temp_dir,
            dest_hook=dest_hook,
            task_id="copy_task",
            dag=test_dag
        )
        _run_task(task, test_dag)

        assert dest_hook.exists(posixpath.join(s3_temp_dir, "test.csv"))


class TestDeleteFileOperator:
    """Tests for the DeleteFileOperator."""

    def test_single(self, s3_client, s3_mock_dir, test_dag):
        """Tests deletion of a single file."""

        hook = S3Hook()

        file_path = posixpath.join(s3_mock_dir, "test.txt")
        assert hook.exists(file_path)

        task = operators.DeleteFileOperator(
            path=file_path,
            hook=hook,
            task_id="copy_task",
            dag=test_dag
        )
        _run_task(task, test_dag)

        assert not hook.exists(file_path)

    def test_glob(self, s3_client, s3_mock_dir, test_dag):
        """Tests deletion of multiple files with glob."""

        hook = S3Hook()

        assert hook.exists(posixpath.join(s3_mock_dir, "test.txt"))
        assert hook.exists(posixpath.join(s3_mock_dir, "test.csv"))

        task = operators.DeleteFileOperator(
            path=posixpath.join(s3_mock_dir, "test.*"),
            hook=hook,
            task_id="copy_task",
            dag=test_dag
        )
        _run_task(task, test_dag)

        assert not hook.exists(posixpath.join(s3_mock_dir, "test.txt"))
        assert not hook.exists(posixpath.join(s3_mock_dir, "test.csv"))

        # Check if other file was not deleted.
        assert hook.exists(posixpath.join(s3_mock_dir, "other.txt"))


class TestDeleteTreeOperator:
    """Tests for the DeleteTreeOperator."""

    def test_single(self, s3_client, s3_mock_dir, test_dag):
        """Tests deletion of a single directory."""

        hook = S3Hook()

        dir_path = posixpath.join(s3_mock_dir, "subdir")
        assert hook.exists(dir_path)

        task = operators.DeleteTreeOperator(
            path=dir_path,
            hook=hook,
            task_id="copy_task",
            dag=test_dag
        )
        _run_task(task, test_dag)

        assert not hook.exists(dir_path)

    def test_glob(self, s3_client, s3_mock_dir, test_dag):
        """Tests deletion of multiple files with glob."""

        hook = S3Hook()

        assert hook.exists(posixpath.join(s3_mock_dir, "subdir"))

        task = operators.DeleteTreeOperator(
            path=posixpath.join(s3_mock_dir, "sub*"),
            hook=hook,
            task_id="copy_task",
            dag=test_dag
        )
        _run_task(task, test_dag)

        assert not hook.exists(posixpath.join(s3_mock_dir, "subdir"))
