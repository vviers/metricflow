import traceback
from typing import Dict, List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricFilter, MetricNode
from metricflow.model.dbt_transformations.dbt_transform_rule import (
    DbtTransformRule,
    DbtTransformedObjects,
    assert_essential_metric_properties,
)
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.metric import MetricInputMeasure, MetricInput, MetricType, MetricTypeParams
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationIssue, ValidationError


CALC_METHOD_TO_METRIC_TYPE: Dict[str, MetricType] = {
    "count": MetricType.MEASURE_PROXY,
    "count_distinct": MetricType.MEASURE_PROXY,
    "sum": MetricType.MEASURE_PROXY,
    "average": MetricType.MEASURE_PROXY,
    "min": MetricType.MEASURE_PROXY,
    "max": MetricType.MEASURE_PROXY,
    "derived": MetricType.DERIVED,
}


class DbtToMetircName(DbtTransformRule):
    """Rule for mapping dbt metric names to metric names"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                objects.metrics[metric.name]["name"] = metric.name

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMetricDescription(DbtTransformRule):
    """Rule for mapping dbt metric descriptions to metric descriptions"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                # metric descriptions are optional
                if metric.description:
                    assert_essential_metric_properties(metric=metric)
                    objects.metrics[metric.name]["description"] = metric.description

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMetricType(DbtTransformRule):
    """Rule for mapping dbt metric calculation_methods to metric types"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                objects.metrics[metric.name]["type"] = CALC_METHOD_TO_METRIC_TYPE[metric.calculation_method]

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureProxyMetricTypeParams(DbtTransformRule):
    """Rule for mapping non-derived dbt metric names to metric measure inputs

    WARNING: This will clobber any other type_params for the metric
    """

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: d
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                # We only do this for MEASURE_PROXY metrics
                if CALC_METHOD_TO_METRIC_TYPE[metric.calculation_method] == MetricType.MEASURE_PROXY:
                    objects.metrics[metric.name]["type_params"] = MetricTypeParams(
                        measure=MetricInputMeasure(name=metric.name)
                    ).dict()

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToDerivedMetricTypeParams(DbtTransformRule):
    """Rule for mapping derived dbt metric depends_on & expression to metric expression and metric inputs

    WARNING: This will clobber any other type_params for the metric
    TODO: We need to take another step to modify the dbt expression to an appropriate MetricFlow expression
    """

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                if (
                    metric.calculation_method
                    and CALC_METHOD_TO_METRIC_TYPE[metric.calculation_method] == MetricType.DERIVED
                ):
                    assert metric.expression, f"Expected an `expression` for `{metric.name}` metric, got `None`"
                    assert metric.depends_on, f"Expected a list `depends_on` for `{metric.name}` metric, got `None`"

                    # `depends_on` is a list of `database.schema.name` strings, we only want the `name` part
                    sub_metrics = [name.split(".")[2] for name in metric.depends_on]
                    metric_inputs = [MetricInput(name=name) for name in sub_metrics]

                    objects.metrics[metric.name]["type_params"] = MetricTypeParams(
                        expr=metric.expression,  # TODO <- Need to make this a MetricFlow expression first
                        metrics=metric_inputs,
                    ).dict()

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMetricConstraint(DbtTransformRule):
    """Rule for mapping dbt metric filters to metric w"""

    @staticmethod
    def _build_where_clause_constraint(filters: List[MetricFilter]) -> WhereClauseConstraint:
        """Builds an SQL 'where' statement from a list of dbt MetricFilters

        Each dbt filter has a field, an operator, and a value. With these dbt
        forms the individual statment '{field} {operator} {value}' and joins
        them with an 'AND'. Thus we do the same.

        Note:
            TODO: We could probably replace this with whatever method dbt uses to
            build the statement.
        """
        clauses = [f"{filter.field} {filter.operator} {filter.value}" for filter in filters]
        return WhereClauseConstraint(where=" AND ".join(clauses), linkable_names=[filter.field for filter in filters])

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                # Filters are optional
                if metric.filters:
                    assert_essential_metric_properties(metric=metric)
                    objects.metrics[metric.name]["constraint"] = DbtToMetricConstraint._build_where_clause_constraint(
                        metric.filters
                    )

            except Exception as e:
                issues.append(ValidationError(message=e, extra_detail="".join(traceback.format_tb(e.__traceback__))))

        return ModelValidationResults.from_issues_sequence(issues=issues)
