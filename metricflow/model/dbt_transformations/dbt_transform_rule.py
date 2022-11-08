from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.validations.validator_helpers import ModelValidationResults


@dataclass
class DbtTransformedObjects:  # type: ignore[misc]
    """Model elements, and sub elements, mapped by element name path"""

    data_sources: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # type: ignore[misc]
    metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # type: ignore[misc]
    materializations: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # type: ignore[misc]
    # access path is ["data_source_name"]["dimension_name"] -> dict dimension representation
    dimensions: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)  # type: ignore[misc]
    # access path is ["data_source_name"]["identifier_name"] -> dict identifier representation
    identifiers: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)  # type: ignore[misc]
    # access path is ["data_source_name"]["measure_name"] -> dict measure representation
    measures: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)  # type: ignore[misc]


@dataclass
class DbtTransformationResult:  # noqa: D
    transformed_objects: DbtTransformedObjects
    issues: ModelValidationResults


class DbtTransformRule(ABC):
    """Encapsulates logic for transforming a dbt manifest. e.g. add metrics based on measures."""

    @staticmethod
    @abstractmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:
        """Take in a DbtTransformedObjects object, transform it in place given the metrics and the rule, return any issues"""
        pass
