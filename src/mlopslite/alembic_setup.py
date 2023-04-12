import os

def get_root_dir():
    """Returns root directory of Mlops-Lite"""
    return os.path.normpath(os.path.dirname(os.path.abspath(__file__)))

def get_alembic_ini():
    """Returns full path to alembic.ini"""
    return os.path.join(get_root_dir(), "registry/alembic.ini")

def get_migration_script_location():
    """Returns full path to migrations"""
    return os.path.join(get_root_dir(), "registry/migration/")
    