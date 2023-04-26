# mlops-lite
Lightweight ML model registry. 

Designed to be lsimple to use portable ML model registry, that embeds (and versions) training data, models and logs model execution. Mainly targetting classification or regression models built with scikit-learn pipelines.

Portability is achieved through attaching to SQLite as a registry storage. So, just by sharing the mlops-lite.db file, it ensures completely reproducable results, as it contains both training data, along with all the models, their settings and split indices that are attached to said training data.

Persistence and ability to scale is achieved by attaching registry to Postgres storage (at the cost of portability). 
