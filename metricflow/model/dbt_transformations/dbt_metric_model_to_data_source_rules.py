import traceback
from typing import List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import ModelNode, MetricNode
from metricflow.model.dbt_transformations.dbt_transform_rule import DbtTransformRule, DbtTransformedObjects
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationIssue, ValidationError


class DbtMapToDataSourceName(DbtTransformRule):
    """Rule for mapping dbt metric model names to data source names"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Derived metrics don't have models, so skip when model doesn't exist
            if metric.model:
                try:
                    assert isinstance(
                        metric.model, ModelNode
                    ), f"Expected `ModelNode` for `{metric.name}` metric's `model`, got `{type(metric.model)}`"
                    assert metric.model.name, f"Expected a `name` for `{metric.name}` metric's `model`, got `None`"
                    objects.add_data_source_object_if_not_exists(metric.model.name)
                    objects.data_sources[metric.model.name]["name"] = metric.model.name

                except Exception as e:
                    issues.append(
                        ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtMapToDataSourceDescription(DbtTransformRule):
    """Rule for mapping dbt metric model descriptions to data source descriptions"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Derived metrics don't have models, so skip when model doesn't exist
            if metric.model:
                try:
                    assert isinstance(
                        metric.model, ModelNode
                    ), f"Expected `ModelNode` for `{metric.name}` metric's `model`, got `{type(metric.model)}`"
                    assert metric.model.name, f"Expected a `name` for `{metric.name}` metric's `model`, got `None`"
                    # Don't need to assert `metric.model.description` because
                    # it's optional and can be set to None
                    objects.add_data_source_object_if_not_exists(metric.model.name)
                    objects.data_sources[metric.model.name]["description"] = metric.model.description

                except Exception as e:
                    issues.append(
                        ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtMapDataSourceSqlTable(DbtTransformRule):
    """Rule for mapping dbt metric models to data source sql tables"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Derived metrics don't have models, so skip when model doesn't exist
            if metric.model:
                try:
                    assert isinstance(
                        metric.model, ModelNode
                    ), f"Expected `ModelNode` for `{metric.name}` metric's `model`, got `{type(metric.model)}`"
                    assert metric.model.name, f"Expected a `name` for `{metric.name}` metric's `model`, got `None`"
                    assert (
                        metric.model.database
                    ), f"Expected a `database` for `{metric.name}` metric's `model`, got `None`"
                    assert metric.model.schema, f"Expected a `schema` for `{metric.name}` metric's `model`, got `None`"
                    objects.add_data_source_object_if_not_exists(metric.model.name)
                    objects.data_sources[metric.model.name][
                        "sql_table"
                    ] = f"{metric.model.database}.{metric.model.schema}.{metric.model.name}"

                except Exception as e:
                    issues.append(
                        ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtMapToDataSourceDbtModel(DbtTransformRule):
    """Rule for mapping dbt metric models to data source dbt models"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Derived metrics don't have models, so skip when model doesn't exist
            if metric.model:
                try:
                    assert isinstance(metric.model, ModelNode)
                    assert metric.model.name, f"Expected a `name` for `{metric.name}` metric's `model`, got `None`"
                    assert (
                        metric.model.database
                    ), f"Expected a `database` for `{metric.name}` metric's `model`, got `None`"
                    assert metric.model.schema, f"Expected a `schema` for `{metric.name}` metric's `model`, got `None`"
                    objects.add_data_source_object_if_not_exists(metric.model.name)
                    objects.data_sources[metric.model.name][
                        "dbt_model"
                    ] = f"{metric.model.database}.{metric.model.schema}.{metric.model.name}"

                except Exception as e:
                    issues.append(
                        ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )
        return ModelValidationResults.from_issues_sequence(issues=issues)
