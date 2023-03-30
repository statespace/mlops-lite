from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Integer, JSON, ForeignKey
import inspect, sys

def get_datamodel_table_names():
    is_class_member = lambda member: inspect.isclass(member) and member.__module__ == __name__
    clsmembers = inspect.getmembers(sys.modules[__name__], is_class_member)
    return [cl.__tablename__ for name, cl in clsmembers if name != "Base"]

class Base(DeclarativeBase):
    pass

class DataRegistry(Base):
    __tablename__ = "data_registry"

    id = Column(Integer, primary_key = True, autoincrement=True)
    name = Column(String)
    version = Column(Integer)
    description = Column(String)
    data = Column(JSON)
    row_size = Column(Integer)
    col_size = Column(Integer)

class DataDefinitions(Base):
    __tablename__ = "data_definitions"

    id = Column(Integer, primary_key = True, autoincrement=True)
    data_id = Column(Integer, ForeignKey('data_registry.id'))
    column = Column(String)
    dtype = Column(String)

