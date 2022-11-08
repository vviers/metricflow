import traceback
from typing import Dict, List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.aggregation_properties import AggregationType
from metricflow.model.dbt_transformations.dbt_transform_rule import (
    DbtTransformRule,
    DbtTransformedObjects,
    assert_essential_metric_properties,
)
from metricflow.model.objects.metric import MetricType
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationIssue, ValidationError
from metricflow.model.dbt_transformations.dbt_metric_to_metrics_rules import CALC_METHOD_TO_METRIC_TYPE

CALC_METHOD_TO_MEASURE_TYPE: Dict[str, AggregationType] = {
    "count": AggregationType.COUNT,
    "count_distinct": AggregationType.COUNT_DISTINCT,
    "sum": AggregationType.SUM,
    "average": AggregationType.AVERAGE,
    "min": AggregationType.MIN,
    "max": AggregationType.MAX,
    # "derived": AggregationType.DERIVED # Derived DBT metrics don't create measures
}


class DbtToMeasureName(DbtTransformRule):
    """Rule for mapping non-derived dbt metric names to data source measure names"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                if CALC_METHOD_TO_METRIC_TYPE[metric.calculation_method] != MetricType.DERIVED:
                    objects.measures[metric.model.name][metric.name]["name"] = metric.name

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureAgg(DbtTransformRule):
    """Rule for mapping non-derived dbt metric calculation method to data source measure agg"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                if CALC_METHOD_TO_METRIC_TYPE[metric.calculation_method] != MetricType.DERIVED:
                    objects.measures[metric.model.name][metric.name]["agg"] = CALC_METHOD_TO_MEASURE_TYPE[
                        metric.calculation_method
                    ]

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureExpr(DbtTransformRule):
    """Rule for mapping non-derived dbt metric expression to data source measure expression"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                if CALC_METHOD_TO_METRIC_TYPE[metric.calculation_method] != MetricType.DERIVED:
                    assert metric.expression, f"Expected an `expression` for `{metric.name}` metric, got `None`"
                    objects.measures[metric.model.name][metric.name]["expr"] = metric.expression

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureAggTimeDimension(DbtTransformRule):
    """Rule for mapping non-derived dbt metric timestamp to data source measure agg_time_dimension"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                if CALC_METHOD_TO_METRIC_TYPE[metric.calculation_method] != MetricType.DERIVED:
                    assert metric.timestamp, f"Expected a `timestamp` for `{metric.name}` metric, got `None`"
                    objects.measures[metric.model.name][metric.name]["agg_time_dimension"] = metric.timestamp

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)
