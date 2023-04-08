import os
from dataclasses import dataclass

DEFAULT_SQLITE_URL = "sqlite:///mlops-lite.db"


@dataclass
class RegistryConfig:
    db_constring: str = DEFAULT_SQLITE_URL
    # fs_type: str
    # fs_root: str
    # db_constring: str = field(init=False)


# from env
def db_constring_from_env():
    # meant for connecting to Postgres, probably needs a config that defines DB choice

    db_type = os.environ["DB_TYPE"]

    if db_type.lower() == "sqlite":
        db_path = os.environ["DB_PATH"]
        return f"{db_type}:///{db_path}"

    else:
        db_user = os.environ["DB_USERNAME"]
        db_pass = os.environ["DB_PASSWORD"]
        db_host = os.environ["DB_HOST"]
        db_port = os.environ["DB_PORT"]
        db_name = os.environ["DB_DATABASE"]

        return f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
