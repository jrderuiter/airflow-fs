from . import FsHook


class S3Hook(FsHook):
    """Hook for interacting with files in S3."""

    def __init__(self, conn_id=None, **kwargs):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None
        self._kwargs = kwargs

    def get_conn(self):
        try:
            import s3fs
        except ImportError:
            raise ImportError("s3fs must be installed to use the S3Hook") from None

        if self._conn is None:
            if self._conn_id is None:
                self._conn = s3fs.S3FileSystem(**self._kwargs)
            else:
                config = self.get_connection(self._conn_id)

                if config.extra_dejson.get("encryption", False):
                    if self._kwargs.get("s3_additional_kwargs"):
                        self._kwargs["s3_additional_kwargs"][
                            "ServerSideEncryption"
                        ] = "AES256"
                    else:
                        self._kwargs["s3_additional_kwargs"] = {
                            "ServerSideEncryption": "AES256"
                        }

                self._conn = s3fs.S3FileSystem(
                    key=config.login, secret=config.password, **self._kwargs
                )
        return self._conn

    def disconnect(self):
        self._conn = None

    def open(self, file_path, mode="rb"):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            raise ValueError("Directory already exists")

    def glob(self, pattern):
        try:
            return self.get_conn().glob(pattern)
        except FileNotFoundError:
            return []

    def remove(self, file_path):
        self.get_conn().rm(file_path, recursive=False)

    def rmtree(self, dir_path):
        self.get_conn().rm(dir_path, recursive=True)
