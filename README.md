# mlops-lite
Lightweight Machine Learning model registry. 

Designed to be a simple to use, portable ML model registry that embeds (and versions) training data, models and logs model execution. Currently implements `sklearn` models, or more precicisely `sklearn.pipeline.Pipeline` objects. 

Portability is achieved through attaching to SQLite as a registry storage. So, just by sharing the mlops-lite.db file, it ensures completely reproducable results, as it contains both training data, along with all the models, their metadata.

Better persistence and ability to scale is achieved by attaching registry to Postgres storage (at the cost of portability). 

## Development notes

In principle it can be used with any object that implements `predict`, `predict_proba`, `feature_names_in_`, `classes_` methods. Future development intends to generalize the interface, but at this stage it mimics `sklearn.pipeline.Pipeline`.

## Usage

Only one dataset and only one model (deployable) can be be active at a time. Binding either to the client will automatically persist it in the database. It will avoid any duplicated artifacts (through comparing md5 hash) and increment version if name is the same but md5 is different.

## Adding dataset to the registry

```python
from mlopslite.client import MlopsLite

# Initializing the client connects to the registry
# By default it's SQLite
client = MlopsLite()

# Get an example Dataset
from mlopslite.utils import sklearn_dataset_to_dataframe
iris = sklearn_dataset_to_dataframe("iris")

# Bind Dataset to the registry
client.bind_dataset(data = iris, name = "iris")
```

## Inspecting the dataset

```python
# Metadata of the currently active dataset
client.dataset.info()
```

|    | column_name       | original_dtype   | converted_dtype   |   null_count |   unique_count |   min_value_num |   max_value_num |
|---:|:------------------|:-----------------|:------------------|-------------:|---------------:|----------------:|----------------:|
|  0 | sepal length (cm) | float64          | float             |            0 |             35 |             4.3 |             7.9 |
|  1 | sepal width (cm)  | float64          | float             |            0 |             23 |             2   |             4.4 |
|  2 | petal length (cm) | float64          | float             |            0 |             43 |             1   |             6.9 |
|  3 | petal width (cm)  | float64          | float             |            0 |             22 |             0.1 |             2.5 |
|  4 | target            | object           | str               |            0 |              3 |           nan   |           nan   |

## Retrieving dataset from the registry

```python
# List all the datasets in the registry
client.list_datasets()
```

|    |   id | name   |   version |   size_cols |   size_rows | description   |
|---:|-----:|:-------|----------:|------------:|------------:|:--------------|
|  0 |    1 | iris   |         1 |           5 |         150 |               |

```python
# the id of dataset from the output of list_datasets method
client.pull_dataset(1)
```

## Example model

From a perspective of `mlopslite` we're only interested in `model` artifact, so in principle this could be any model that is trained with `sklearn` and assembled as a `Pipeline`.

```python
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    client.dataset.data.drop('target', axis = 1), 
    client.dataset.data['target']
)
column_def = client.dataset.info()
num_cols = column_def.loc[column_def['converted_dtype'] == 'float', 'column_name'].to_list()

num_transform = make_pipeline(
    SimpleImputer(strategy="median"), 
    StandardScaler()
)

preproc = ColumnTransformer([
    ('num', num_transform, num_cols),
])

model = make_pipeline(preproc, LogisticRegression())
model.fit(X = X_train, y = y_train)
```

## Adding model to the registry

This will persist the model in the database. Similarly like dataset, it will check md5 of model artifact and will not write duplicates. And will increment version if artifact is diferent but name is the same.

```python
client.bind_model(object = model, name = 'testmodel', target = 'target')
```
Similar to datasets, list all models in the registry:

```python
client.list_models()
```
|    |   id | name      |   version |   dataset_registry_id | estimator_class                                             | estimator_type   | description   |
|---:|-----:|:----------|----------:|----------------------:|:------------------------------------------------------------|:-----------------|:--------------|
|  0 |    1 | testmodel |         1 |                     1 | <class 'sklearn.linear_model._logistic.LogisticRegression'> | classifier       |               |


## Model execution 

Assuming this is a new python session.

```python
from mlopslite.client import MlopsLite
client = MlopsLite()

test = [
    {
        'sepal length (cm)': 2,
        'petal length (cm)': 1,
        'petal width (cm)': 1,
        'sepal width (cm)': 1
    },
    {
        'reference_id': 'some_external_id',
        'sepal length (cm)': 1,
        'petal length (cm)': 2,
        'petal width (cm)': 1,
        'sepal width (cm)': 1
    }
]

client.model.predict(test, log = True)

"""
output:

[{'reference_id': None,
  'results': {'setosa': 0.7770369931272464,
   'versicolor': 0.22263517173716368,
   'virginica': 0.00032783513558999345}},
 {'reference_id': 'some_external_id',
  'results': {'setosa': 0.9021059071941613,
   'versicolor': 0.0973836569186195,
   'virginica': 0.0005104358872192664}}]
"""
```

All inputs and outputs are logged in the registry, with a reference to `Dataset` and `Deployable` artifacts. 

## TODO

### General
- Documentation
- Minor refactor (consistently naming things)
- Testing (most used models & transformation patterns; SQLite+Postgres+MySQL)
- Build pipelines (Github actions)
- Package & Publish on PyPi, version 0.0.1 
- Example deployment setup suggestion

### Features
- Monitoring interface
- State consistency (e.g. unloading model when other dataset is loaded; switching appropriate dataset when other model is loaded etc.)
- Extract library dependencies and write to deployable metadata + check upon pulling model.
- Alternative Dataset file types (JSON, csv, parquet)
- Alternative Deployable file types (pickle, joblib, PMML)
- Alternative artifact save targets (file system e.g. AWS, with reference in registry)