from mlopslite.artifacts.dataset import DataSet
from dataclasses import dataclass
from sklearn.base import is_classifier, is_regressor
from sklearn.pipeline import Pipeline
import pickle
from typing import Any
from hashlib import md5
from pydantic import create_model
from pydantic.main import ModelMetaclass
import pandas as pd

@dataclass
class DeployableMetadata:
    id: int | None
    dataset_registry_id: int
    name: str
    version: int | None
    target: str
    target_mapping: dict | None
    description: str
    estimator_type: str
    estimator_class: str
    variables: dict[str, Any]

    def get_variables(self):
        return self.variables


@dataclass
class Deployable:

    deployable: Pipeline
    metadata: DeployableMetadata

    @staticmethod
    def create( 
            deployable: Pipeline, 
            dataset: DataSet, 
            name: str, 
            target: str, 
            target_mapping: dict | None = None, 
            description: str = ""
    ) -> 'Deployable':

        # potentially can implement various types of deployables
        # for now, extremely coupled with sklearn Pipeline
        # expecting BaseEstimator as last element
        
        verify_deployable(deployable=deployable)

        metadata = {
            'id': None,
            'dataset_registry_id': dataset.metadata.id,
            'name': name,
            'version': None,
            'target': target,
            'target_mapping': target_mapping,
            'description': description,
            'estimator_type': get_estimator_type(deployable),
            'estimator_class': str(deployable[-1].__class__),
            'variables': variable_metadata(dataset=dataset, deployable=deployable)
        }

        return Deployable(deployable, DeployableMetadata(**metadata))
    
    @staticmethod
    def restore(
        registry_item: dict
    ) -> 'Deployable':
        
        metadata = {key: val for key,
            val in registry_item.items() if key in DeployableMetadata.__annotations__.keys()}
        
        deployable = pickle.loads(registry_item['deployable'])
        # check hash?
        return Deployable(deployable, DeployableMetadata(**metadata))

    @property
    def classes(self):
        return self.deployable.classes_
    
    @property
    def estimator_class(self):
        return str(self.deployable[-1].__class__)
    
    def serialize_deployable(self) -> bytes:
        return pickle.dumps(self.deployable)
    
    def get_data_hash(self) -> str:
        return md5(self.serialize_deployable()).hexdigest()
    
    def construct_pydantic_model(self) -> ModelMetaclass:


        variable_definition = self.metadata.get_variables()

        class Config:
            extra = 'forbid'

        schema_definition = {k: (eval(v), None) for k,v in variable_definition.items()}
        schema_definition['reference_id'] = (str, None)

        generated_schema = create_model(
            'inferred_model', 
            **schema_definition, 
            __config__=Config
        )

        return generated_schema
    
    def predict(self, input: list[dict]):

        # validation
        validator_schema = self.construct_pydantic_model()
        input = [validator_schema(**i).__dict__ for i in input]

        refid_list = [i['reference_id'] for i in input]

        # convert back to DataFrame..? There should be a better way to convert back and forth
        input = pd.DataFrame(input)

        if get_estimator_type(self.deployable) == 'classifier':

            model_response = self.deployable.predict_proba(input)

            output = [
                {
                    'reference_id': i[0], 
                    'results': dict(zip(self.classes, i[1]))
                } for i in zip(refid_list, model_response)
            ]
        else:
            model_response = self.deployable.predict(input)

            output = [
                {
                    'reference_id': i[0], 
                    'results': i[1]
                } for i in zip(refid_list, model_response)
            ]

        return output

### Function block ###
    

def verify_dataset(metadata: DeployableMetadata, dataset: DataSet):

    """
    Check if dataset contains all input variables and target
    Check if target_mapping (if exists) corresponds to target values
    """

    ds_cols = [i.column_name for i in dataset.metadata.column_metadata]
    
    if not metadata.target in ds_cols:
        raise ValueError(f'Target ({metadata.target}) not found in dataset columns: ({ds_cols})')
    
    if not all([i in ds_cols for i in metadata.variables.keys()]):
        raise ValueError(f'Variables ({metadata.variables.keys()}) not found in dataset columns: ({ds_cols})')
    
    if metadata.target_mapping is not None:
        if not all([i in metadata.target_mapping.keys() for i in dataset.data[metadata.target].unique()]):
            raise ValueError(
                f'Target mapping {metadata.target_mapping.keys()} does not correspond to target values {dataset.data[metadata.target].unique()}'
                )

def verify_deployable(deployable):

    if not isinstance(deployable, Pipeline):
        raise ValueError('Invalid deployable, expected: sklearn.pipeline.Pipeline')

    if not hasattr(deployable, 'predict'):
        raise ValueError('Deployable does not implement predict method')
    
    if not hasattr(deployable, 'feature_names_in_'):
        raise ValueError('Deployable does not implement feature_names_in_ property')
    
    if is_classifier(deployable):

        if not hasattr(deployable, 'predict_proba'):
            raise ValueError('Deployable does not implement predict_proba method')
        
        if not hasattr(deployable, 'classes_'):
            raise ValueError('Deployable does not implement classes_ property') 

def get_estimator_type(deployable):
    if is_classifier(deployable):
        return 'classifier'
    if is_regressor(deployable):
        return 'regressor'
    return 'unknown'

def variable_metadata(deployable: Pipeline, dataset: DataSet):
    mod_vars = deployable.feature_names_in_
    col_meta = dataset.metadata.column_metadata

    return {i.column_name: i.converted_dtype for i in col_meta if i.column_name in mod_vars}



    

