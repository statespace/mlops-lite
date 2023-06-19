import inspect
import sys
from typing import Any, List
from datetime import datetime

from sqlalchemy import JSON, ForeignKey, UniqueConstraint
from sqlalchemy.orm import (DeclarativeBase, Mapped, MappedAsDataclass,
                            mapped_column, relationship)


def get_datamodel_table_names():
    is_class_member = (
        lambda member: inspect.isclass(member) and member.__module__ == __name__
    )
    clsmembers = inspect.getmembers(sys.modules[__name__], is_class_member)
    return [cl.__tablename__ for name, cl in clsmembers if name != "Base"]


class Base(DeclarativeBase, MappedAsDataclass):
    type_annotation_map = {dict[str, Any]: JSON}


class DatasetRegistry(Base):
    __tablename__ = "dataset_registry"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    name: Mapped[str]
    version: Mapped[int]
    description: Mapped[str]
    data: Mapped[dict[str, Any]]
    size_cols: Mapped[int]
    size_rows: Mapped[int]
    hash: Mapped[str] = mapped_column(unique=True)
    created_at: Mapped[datetime]

    columns: Mapped[List["DatasetRegistryColumns"]] = relationship(
        default_factory=list, back_populates="data", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("name", "version"),)


class DatasetRegistryColumns(Base):
    __tablename__ = "dataset_registry_columns"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    dataset_registry_id: Mapped[int] = mapped_column(
        ForeignKey("dataset_registry.id"), init=False
    )
    column_name: Mapped[str]
    original_dtype: Mapped[str]
    converted_dtype: Mapped[str]
    null_count: Mapped[int]
    unique_count: Mapped[int]
    min_value_num: Mapped[float] = mapped_column(nullable=True)
    max_value_num: Mapped[float] = mapped_column(nullable=True)
    #unique_values: Mapped[list[str]]

    data: Mapped["DatasetRegistry"] = relationship(back_populates="columns")

class DeployableRegistry(Base):
    __tablename__ = "deployable_registry"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    dataset_registry_id: Mapped[int] = mapped_column(
        ForeignKey("dataset_registry.id")
    )
    name: Mapped[str]
    version: Mapped[int]
    target: Mapped[str]
    target_mapping: Mapped[dict[str, Any] | None]
    description: Mapped[str]
    estimator_type: Mapped[str]
    estimator_class: Mapped[str]
    deployable: Mapped[bytes]
    variables: Mapped[dict[str, Any]]
    hash: Mapped[str] = mapped_column(unique=True)
    created_at: Mapped[datetime]

class ExecutionLog(Base):
    __tablename__ = "model_execution_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    deployable_id: Mapped[int] = mapped_column(ForeignKey("deployable_registry.id"))
    request_time: Mapped[datetime]
    request_size: Mapped[int]

    execution_log: Mapped[List["ExecutionItems"]] = relationship(
        default_factory=list, back_populates="execution_log_item", cascade="all, delete-orphan"
    )

class ExecutionItems(Base):
    __tablename__ = "execution_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    execution_log_id: Mapped[int] = mapped_column(ForeignKey("model_execution_log.id"), init=False)
    reference_id: Mapped[str] = mapped_column(nullable=True)

    execution_log_item: Mapped["ExecutionLog"] = relationship(back_populates="execution_log")
    
    request_items: Mapped[List["RequestItems"]] = relationship(
        default_factory=list, back_populates="data", cascade="all, delete-orphan"
    )

    response_items: Mapped[List["ResponseItems"]] = relationship(
        default_factory=list, back_populates="data", cascade="all, delete-orphan"
    )

class RequestItems(Base):
    __tablename__ = "request_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    execution_items_id: Mapped[int] = mapped_column(ForeignKey("execution_items.id"), init=False)
    varname: Mapped[str]
    in_value: Mapped[str]

    data: Mapped["ExecutionItems"] = relationship(back_populates="request_items")

class ResponseItems(Base):
    __tablename__ = "response_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    execution_items_id: Mapped[int] = mapped_column(ForeignKey("execution_items.id"), init=False)
    classname: Mapped[str] = mapped_column(nullable=True)
    out_value: Mapped[float]

    data: Mapped["ExecutionItems"] = relationship(back_populates="response_items")




