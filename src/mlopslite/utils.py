from sklearn.datasets import load_iris, load_wine
import pandas as pd
import numpy as np
from mlopslite.alembic_setup import get_migration_script_location, get_alembic_ini
import os
from glob import glob
from alembic.config import Config
from alembic import command
from mlopslite.registry.registryconfig import RegistryConfig
from sqlalchemy import create_engine
from time import time

# these are dev utils, and probably should be removed at some point

def sklearn_dataset_to_dataframe(x: str):

    sklearn_datasets = {
        'iris': load_iris(), 
        'wine': load_wine()
    }

    dataset = sklearn_datasets[x]

    #iris_raw = load_iris(as_frame=True)
    out = pd.DataFrame(data= np.c_[dataset['data'], dataset['target']],
                        columns= dataset['feature_names'] + ['target'])
    out['target'] = out['target'].map({i:v for i,v in enumerate(dataset['target_names'])})
    return out

def reset_registry(regconf: RegistryConfig):

    # remove migrations scripts
    migration_scripts = get_migration_script_location()
    version_scripts = os.path.join(migration_scripts, 'versions/')

    for i in glob(f"{version_scripts}*.py"):
        os.remove(i)

    db_file = os.path.join(os.getcwd(), 'mlops-lite.db')
    if os.path.exists(db_file):
        os.remove(db_file)


    # recreate db & regen migration

    engine = create_engine(regconf.db_constring)

    config = Config(get_alembic_ini())
    config.set_main_option("script_location", get_migration_script_location())
    config.set_main_option("sqlalchemy.url", regconf.db_constring)

    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.revision(config, f"{int(time())}_update", autogenerate=True) 

