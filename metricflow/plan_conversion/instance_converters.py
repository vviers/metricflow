"""Various classes that perform a transformation on instance sets."""

from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass
from itertools import chain
from more_itertools import bucket
from typing import Dict, List, Optional, Sequence, Tuple

from metricflow.aggregation_properties import AggregationState
from metricflow.dataflow.dataflow_plan import ValidityWindowJoinDescription
from metricflow.instances import (
    MdoInstance,
    DimensionInstance,
    IdentifierInstance,
    MetricInstance,
    MeasureInstance,
    InstanceSet,
    InstanceSetTransform,
    TimeDimensionInstance,
)
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.object_utils import assert_exactly_one_arg_set
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.specs import (
    MeasureSpec,
    MetricInputMeasureSpec,
    InstanceSpec,
    IdentifierSpec,
    DimensionSpec,
    ColumnAssociationResolver,
    TimeDimensionSpec,
    LinklessIdentifierSpec,
    LinkableInstanceSpec,
    IdentifierReference,
)
from metricflow.sql.sql_exprs import (
    SqlColumnReferenceExpression,
    SqlColumnReference,
    SqlFunctionExpression,
)
from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class CreateSelectColumnsForInstances(InstanceSetTransform[SelectColumnSet]):
    """Create select column expressions that will express all instances in the set.

    It assumes that the column names of the instances are represented by the supplied column association resolver and
    come from the given table alias.
    """

    def __init__(
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        output_to_input_column_mapping: Optional[OrderedDict[str, str]] = None,
    ) -> None:
        """Initializer.

        Args:
            table_alias: the table alias to select columns from
            column_resolver: resolver to name columns.
            output_to_input_column_mapping: if specified, use these columns in the input for the given output columns.
        """
        self._table_alias = table_alias
        self._column_resolver = column_resolver
        self._output_to_input_column_mapping = output_to_input_column_mapping or OrderedDict()

    def transform(self, instance_set: InstanceSet) -> SelectColumnSet:  # noqa: D
        metric_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.metric_instances])
        )
        measure_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.measure_instances])
        )
        dimension_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.dimension_instances])
        )
        time_dimension_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.time_dimension_instances])
        )
        identifier_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.identifier_instances])
        )
        return SelectColumnSet(
            metric_columns=metric_cols,
            measure_columns=measure_cols,
            dimension_columns=dimension_cols,
            time_dimension_columns=time_dimension_cols,
            identifier_columns=identifier_cols,
        )

    def _make_sql_column_expression(
        self,
        element_instance: MdoInstance,
    ) -> List[SqlSelectColumn]:
        """Convert one element instance into a SQL column"""

        # Do a sanity check to make sure that there's a 1:1 mapping between the columns associations generated by the
        # column resolver based on the spec, and the columns that are already associated with the instance.
        expected_column_associations = element_instance.spec.column_associations(self._column_resolver)
        existing_column_associations = element_instance.associated_columns

        # Dict between the expected column name and the corresponding column in the existing columns
        column_matches: Dict[str, List[str]] = {
            expected_column.column_name: [
                col.column_name
                for col in existing_column_associations
                if col.column_correlation_key == expected_column.column_correlation_key
            ]
            for expected_column in expected_column_associations
        }

        # Assert a 1:1 mapping between expected and existing
        assert all([len(x) == 1 for x in column_matches.values()]), (
            f"Did not find exactly one match for each expected column associations.  "
            f"Expected -> existing mappings: {column_matches}"
        )
        existing_names = set([col.column_name for col in existing_column_associations])
        mapped_names = set()
        mapped_cols: List[str] = []
        for mapped_cols in column_matches.values():
            mapped_names.update([col_name for col_name in mapped_cols])
        assert existing_names == mapped_names, (
            f"Not all existing columns were mapped. Existing: {existing_names}.  Mapped: {mapped_cols}, "
            f"{expected_column_associations} -- {existing_column_associations}"
        )

        select_columns = []
        for expected_name, mapped_cols in column_matches.items():
            input_column_name = mapped_cols[0]
            output_column_name = expected_name

            if output_column_name in self._output_to_input_column_mapping:
                input_column_name = self._output_to_input_column_mapping[output_column_name]
            select_columns.append(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(SqlColumnReference(self._table_alias, input_column_name)),
                    column_alias=output_column_name,
                )
            )
        return select_columns


class CreateSelectColumnsWithMeasuresAggregated(CreateSelectColumnsForInstances):
    """Create select columns of the form "fct_bookings.bookings AS bookings" for all the instances.

    However, for measure columns, convert them into expressions like "SUM(fct_bookings.bookings) AS bookings" so that
    the resulting expressions can be used for aggregations.

    Also add an output alias that conforms to the alias
    """

    def __init__(  # noqa: D
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        data_source_semantics: DataSourceSemanticsAccessor,
        metric_input_measure_specs: Sequence[MetricInputMeasureSpec],
    ) -> None:
        self._data_source_semantics = data_source_semantics
        self.metric_input_measure_specs = metric_input_measure_specs
        super().__init__(table_alias=table_alias, column_resolver=column_resolver)

    def _make_sql_column_expression_to_aggregate_measures(
        self, measure_instances: Tuple[MeasureInstance, ...]
    ) -> List[SqlSelectColumn]:
        output_columns: List[SqlSelectColumn] = []
        aliased_input_specs = [spec for spec in self.metric_input_measure_specs if spec.alias]
        for instance in measure_instances:
            matches = [spec for spec in aliased_input_specs if spec.measure_spec == instance.spec]
            if matches:
                aliased_spec = matches[0]
                aliased_input_specs.remove(aliased_spec)
                output_measure_spec = aliased_spec.post_aggregation_spec
            else:
                output_measure_spec = instance.spec

            output_columns.append(
                self._make_sql_column_expression_to_aggregate_measure(
                    measure_instance=instance, output_measure_spec=output_measure_spec
                )
            )

        return output_columns

    def _make_sql_column_expression_to_aggregate_measure(  # noqa: D
        self, measure_instance: MeasureInstance, output_measure_spec: MeasureSpec
    ) -> SqlSelectColumn:
        """Convert one measure instance into a SQL column"""
        # Get the column name of the measure in the table that we're reading from
        column_name_in_table = measure_instance.associated_column.column_name

        # Create an expression that will aggregate the given measure.
        # Figure out the aggregation function for the measure.
        measure = self._data_source_semantics.get_measure(measure_instance.spec.as_reference)
        aggregation_type = measure.agg

        expression_to_get_measure = SqlColumnReferenceExpression(
            SqlColumnReference(self._table_alias, column_name_in_table)
        )

        expression_to_aggregate_measure = SqlFunctionExpression.from_aggregation_type(
            aggregation_type=aggregation_type, sql_column_expression=expression_to_get_measure
        )

        # Get the output column name from the measure/alias

        new_column_association_for_aggregated_measure = self._column_resolver.resolve_measure_spec(output_measure_spec)
        new_column_name_for_aggregated_measure = new_column_association_for_aggregated_measure.column_name

        return SqlSelectColumn(
            expr=expression_to_aggregate_measure,
            column_alias=new_column_name_for_aggregated_measure,
        )

    def transform(self, instance_set: InstanceSet) -> SelectColumnSet:  # noqa: D
        metric_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.metric_instances])
        )

        measure_cols = self._make_sql_column_expression_to_aggregate_measures(instance_set.measure_instances)
        dimension_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.dimension_instances])
        )
        time_dimension_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.time_dimension_instances])
        )
        identifier_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.identifier_instances])
        )
        return SelectColumnSet(
            metric_columns=metric_cols,
            measure_columns=measure_cols,
            dimension_columns=dimension_cols,
            time_dimension_columns=time_dimension_cols,
            identifier_columns=identifier_cols,
        )


@dataclass(frozen=True)
class _DimensionValidityParams:
    """Helper dataclass for managing dimension validity properties"""

    dimension_name: str
    time_granularity: TimeGranularity


class CreateValidityWindowJoinDescription(InstanceSetTransform[Optional[ValidityWindowJoinDescription]]):
    """Create and return a ValidityWindowJoinDescription based on the given InstanceSet

    During join resolution we need to determine whether or not a given data set represents a
    Type II SCD dataset - i.e., one with a validity window defined on each row. This requires
    checking the set of dimension instances and determining whether or not those originate from
    an SCD source, and extracting validity window information accordingly.
    """

    def __init__(self, data_source_semantics: DataSourceSemanticsAccessor) -> None:
        """Initializer. The DataSourceSemanticsAccessor is needed for getting the original model definition."""
        self._data_source_semantics = data_source_semantics

    def _get_validity_dimensions_for_data_source(
        self, data_source_name: str
    ) -> Optional[Tuple[_DimensionValidityParams, _DimensionValidityParams]]:
        """Returns a 2-tuple (start, end) of validity window dimensions info, if any exist in the data source"""
        data_source = self._data_source_semantics.get(data_source_name=data_source_name)
        assert data_source, f"Could not find data source with name {data_source_name} after data set conversion!"

        validity_dims = [dim for dim in data_source.dimensions if dim.validity_params is not None]

        if not validity_dims:
            return None

        start_dims = [dim for dim in validity_dims if dim.validity_params and dim.validity_params.is_start]
        end_dims = [dim for dim in validity_dims if dim.validity_params and dim.validity_params.is_end]
        assert len(start_dims) == 1, (
            f"Error, found more than one validity window start dimension in data source `{data_source_name}`. "
            f"This should have been blocked by model validation! Start dims: `{start_dims}`"
        )
        assert len(end_dims) == 1, (
            f"Error, found more than one validity window end dimension in data source `{data_source_name}`. "
            f"This should have been blocked by model validation! End dims: `{end_dims}`"
        )
        start_dim = start_dims[0]
        end_dim = end_dims[0]

        # TODO: clean up type_params optional state for Dimension (time dimension?) objects
        assert start_dim.type_params, "Typechecker hint - validity info cannot exist without the type params"
        assert end_dim.type_params, "Typechecker hint - validity info cannot exist without the type params"

        return (
            _DimensionValidityParams(
                dimension_name=start_dim.name, time_granularity=start_dim.type_params.time_granularity
            ),
            _DimensionValidityParams(
                dimension_name=end_dim.name, time_granularity=end_dim.type_params.time_granularity
            ),
        )

    def transform(self, instance_set: InstanceSet) -> Optional[ValidityWindowJoinDescription]:
        """Return find the Time Dimension specs defining a validity window, if any, and return it

        This currently throws an exception if more than one such window is found, and effectively prevents
        us from processing a dataset composed of a join between two SCD data sources. This restriction is in
        place as a temporary simplification - if there is need for this feature we can enable it.
        """
        data_source_to_window: Dict[str, ValidityWindowJoinDescription] = {}
        instances_by_data_source = bucket(
            instance_set.time_dimension_instances, lambda x: x.origin_data_source_reference.data_source_name
        )
        for data_source_name in instances_by_data_source:
            validity_dims = self._get_validity_dimensions_for_data_source(data_source_name=data_source_name)
            if validity_dims is None:
                continue

            start_dim, end_dim = validity_dims
            specs = {instance.spec for instance in instances_by_data_source[data_source_name]}
            start_specs = [
                spec
                for spec in specs
                if spec.element_name == start_dim.dimension_name and spec.time_granularity == start_dim.time_granularity
            ]
            end_specs = [
                spec
                for spec in specs
                if spec.element_name == end_dim.dimension_name and spec.time_granularity == end_dim.time_granularity
            ]
            linkless_start_specs = {spec.without_identifier_links() for spec in start_specs}
            linkless_end_specs = {spec.without_identifier_links() for spec in end_specs}
            assert len(linkless_start_specs) == 1 and len(linkless_end_specs) == 1, (
                f"Did not find exactly one pair of specs from data source `{data_source_name}` matching the validity "
                f"window end points defined in the data source. This means we cannot process an SCD join, because we "
                f"require exactly one validity window to be specified for the query! The window in the data source "
                f"is defined by start dimension `{start_dim}` and end dimension `{end_dim}`. We found "
                f"{len(linkless_start_specs)} linkless specs for window start ({linkless_start_specs}) and "
                f"{len(linkless_end_specs)} linkless specs for window end ({linkless_end_specs})."
            )
            # SCD join targets are joined as dimension links in much the same was as partitions are joined. Therefore,
            # we treat this like a partition time column join and take the dimension spec with the shortest set of
            # identifier links so that the subquery uses the correct reference in the ON statement
            start_specs = sorted(start_specs, key=lambda x: len(x.identifier_links))
            end_specs = sorted(end_specs, key=lambda x: len(x.identifier_links))
            data_source_to_window[data_source_name] = ValidityWindowJoinDescription(
                window_start_dimension=start_specs[0], window_end_dimension=end_specs[0]
            )

        assert len(data_source_to_window) < 2, (
            f"Found more than 1 set of validity window specs in the input instance set. This is not currently "
            f"supported, as joins between SCD data sources are not yet allowed! {data_source_to_window}"
        )

        if data_source_to_window:
            return list(data_source_to_window.values())[0]

        return None


class AddLinkToLinkableElements(InstanceSetTransform[InstanceSet]):
    """Return a new instance set where the all linkable elements in the set have a new link added.

    e.g. "country" -> "user_id__country" after a data set has been joined by identifier.
    """

    def __init__(self, join_on_identifier: LinklessIdentifierSpec) -> None:  # noqa: D
        self._join_on_identifier = join_on_identifier

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        assert len(instance_set.metric_instances) == 0, "Can't add links to instance sets with metrics"
        assert len(instance_set.measure_instances) == 0, "Can't add links to instance sets with measures"

        # Handle dimension instances
        dimension_instances_with_additional_link = []
        for dimension_instance in instance_set.dimension_instances:
            # The new dimension spec should include the join on identifier.
            transformed_dimension_spec_from_right = DimensionSpec(
                element_name=dimension_instance.spec.element_name,
                identifier_links=self._join_on_identifier.as_linkless_prefix + dimension_instance.spec.identifier_links,
            )
            dimension_instances_with_additional_link.append(
                DimensionInstance(
                    associated_columns=dimension_instance.associated_columns,
                    defined_from=dimension_instance.defined_from,
                    spec=transformed_dimension_spec_from_right,
                )
            )

        # Handle time dimension instances
        time_dimension_instances_with_additional_link = []
        for time_dimension_instance in instance_set.time_dimension_instances:
            # The new dimension spec should include the join on identifier.
            transformed_time_dimension_spec_from_right = TimeDimensionSpec(
                element_name=time_dimension_instance.spec.element_name,
                identifier_links=(
                    (IdentifierReference(element_name=self._join_on_identifier.element_name),)
                    + time_dimension_instance.spec.identifier_links
                ),
                time_granularity=time_dimension_instance.spec.time_granularity,
            )
            time_dimension_instances_with_additional_link.append(
                TimeDimensionInstance(
                    associated_columns=time_dimension_instance.associated_columns,
                    defined_from=time_dimension_instance.defined_from,
                    spec=transformed_time_dimension_spec_from_right,
                )
            )

        # Handle identifier instances
        identifier_instances_with_additional_link = []
        for identifier_instance in instance_set.identifier_instances:
            # Don't include adding the identifier link to the same identifier.
            # Otherwise, you would create "user_id__user_id", which is confusing.
            if identifier_instance.spec == self._join_on_identifier:
                continue
            # The new identifier spec should include the join on identifier.
            transformed_identifier_spec_from_right = IdentifierSpec(
                element_name=identifier_instance.spec.element_name,
                identifier_links=self._join_on_identifier.as_linkless_prefix
                + identifier_instance.spec.identifier_links,
            )
            identifier_instances_with_additional_link.append(
                IdentifierInstance(
                    associated_columns=identifier_instance.associated_columns,
                    defined_from=identifier_instance.defined_from,
                    spec=transformed_identifier_spec_from_right,
                )
            )

        return InstanceSet(
            measure_instances=(),
            dimension_instances=tuple(dimension_instances_with_additional_link),
            time_dimension_instances=tuple(time_dimension_instances_with_additional_link),
            identifier_instances=tuple(identifier_instances_with_additional_link),
            metric_instances=(),
        )


class FilterLinkableInstancesWithLeadingLink(InstanceSetTransform[InstanceSet]):
    """Return an instance set with the elements that have a specified leading link removed.

    e.g. Remove "listing__country" if the specified link is "listing".
    """

    def __init__(  # noqa: D
        self,
        identifier_link: LinklessIdentifierSpec,
    ) -> None:
        """Constructor.

        Args:
            identifier_link: Remove elements with this link as the first element in "identifier_links"
        """
        self._identifier_link = identifier_link

    def _should_pass(self, linkable_spec: LinkableInstanceSpec) -> bool:  # noqa: D
        return (
            len(linkable_spec.identifier_links) == 0
            or LinklessIdentifierSpec.from_reference(linkable_spec.identifier_links[0]) != self._identifier_link
        )

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D

        # Normal to not filter anything if the instance set has no instances with links.
        filtered_dimension_instances = tuple(x for x in instance_set.dimension_instances if self._should_pass(x.spec))
        filtered_time_dimension_instances = tuple(
            x for x in instance_set.time_dimension_instances if self._should_pass(x.spec)
        )
        filtered_identifier_instances = tuple(x for x in instance_set.identifier_instances if self._should_pass(x.spec))

        output = InstanceSet(
            measure_instances=instance_set.measure_instances,
            dimension_instances=filtered_dimension_instances,
            time_dimension_instances=filtered_time_dimension_instances,
            identifier_instances=filtered_identifier_instances,
            metric_instances=instance_set.metric_instances,
        )
        return output


class FilterElements(InstanceSetTransform[InstanceSet]):
    """Return an instance set with the elements that don't match any of the pass specs removed."""

    def __init__(  # noqa: D
        self,
        include_specs: Optional[Sequence[InstanceSpec]] = None,
        exclude_specs: Optional[Sequence[InstanceSpec]] = None,
    ) -> None:
        """Constructor.

        Args:
            include_specs: If specified, pass only instances matching these specs.
            exclude_specs: If specified, pass only instances not matching these specs.
        """
        assert_exactly_one_arg_set(include_specs=include_specs, exclude_specs=exclude_specs)
        self._include_specs = include_specs
        self._exclude_specs = exclude_specs

    def _should_pass(self, element_spec: InstanceSpec) -> bool:  # noqa: D
        # TODO: Use better matching function
        if self._include_specs:
            return any(x == element_spec for x in self._include_specs)
        elif self._exclude_specs:
            return not any(x == element_spec for x in self._exclude_specs)
        assert False

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        # Sanity check to make sure the specs are in the instance set

        if self._include_specs:
            for include_spec in self._include_specs:
                if include_spec not in instance_set.spec_set.all_specs:
                    raise RuntimeError(
                        f"Include spec {include_spec} is not in the spec set {instance_set.spec_set} - "
                        f"check if this node was constructed correctly."
                    )
        elif self._exclude_specs:
            for exclude_spec in self._exclude_specs:
                if exclude_spec not in instance_set.spec_set.all_specs:
                    raise RuntimeError(
                        f"Exclude spec {exclude_spec} is not in the spec set {instance_set.spec_set} - "
                        f"check if this node was constructed correctly."
                    )
        else:
            assert False, "Include specs or exclude specs should have been specified."

        output = InstanceSet(
            measure_instances=tuple(x for x in instance_set.measure_instances if self._should_pass(x.spec)),
            dimension_instances=tuple(x for x in instance_set.dimension_instances if self._should_pass(x.spec)),
            time_dimension_instances=tuple(
                x for x in instance_set.time_dimension_instances if self._should_pass(x.spec)
            ),
            identifier_instances=tuple(x for x in instance_set.identifier_instances if self._should_pass(x.spec)),
            metric_instances=tuple(x for x in instance_set.metric_instances if self._should_pass(x.spec)),
        )
        return output


class ChangeMeasureAggregationState(InstanceSetTransform[InstanceSet]):
    """Returns a new instance set where all measures are set as a different aggregation state."""

    def __init__(self, aggregation_state_changes: Dict[AggregationState, AggregationState]) -> None:
        """Constructor.

        Args:
            aggregation_state_changes: key is old aggregation state, value is the new aggregation state.
        """
        self._aggregation_state_changes = aggregation_state_changes

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        for measure_instance in instance_set.measure_instances:
            assert measure_instance.aggregation_state in self._aggregation_state_changes, (
                f"Aggregation state: {measure_instance.aggregation_state} not handled in change dict: "
                f"{self._aggregation_state_changes}"
            )

        # Copy the measures, but just change the aggregation state to COMPLETE.
        measure_instances = tuple(
            MeasureInstance(
                associated_columns=x.associated_columns,
                defined_from=x.defined_from,
                aggregation_state=self._aggregation_state_changes[x.aggregation_state],
                spec=x.spec,
            )
            for x in instance_set.measure_instances
        )
        return InstanceSet(
            measure_instances=measure_instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            identifier_instances=instance_set.identifier_instances,
            metric_instances=instance_set.metric_instances,
        )


class AliasAggregatedMeasures(InstanceSetTransform[InstanceSet]):
    """Returns a new instance set where all measures have been assigned an alias spec"""

    def __init__(self, metric_input_measure_specs: Sequence[MetricInputMeasureSpec]):
        """Initializer stores the input specs, which contain the aliases for each measure

        Note this class only works if used in conjunction with an AggregateMeasuresNode that has been generated
        by querying a single data source for a single set of aggregated measures. This is currently enforced
        by the structure of the DataflowPlanBuilder, which ensures each AggregateMeasuresNode corresponds to
        a single data source set of measures for a single metric, and that these outputs will then be
        combinded via joins.
        """
        self.metric_input_measure_specs = metric_input_measure_specs

    def _alias_measure_instances(self, measure_instances: Tuple[MeasureInstance, ...]) -> Tuple[MeasureInstance, ...]:
        """Update all measure instances with aliases, if any are found in the input spec set"""
        aliased_instances: List[MeasureInstance] = []
        aliased_input_specs = [spec for spec in self.metric_input_measure_specs if spec.alias]
        for instance in measure_instances:
            matches = [spec for spec in aliased_input_specs if spec.measure_spec == instance.spec]
            assert (
                len(matches) < 2
            ), f"Found duplicate aliased measure spec matches: {matches} for measure instance {instance}. "
            "We should always have 0 or 1 matches, or else we might pass the wrong aggregated measures to the "
            "downstream metric computation expression!"
            if matches:
                aliased_spec = matches[0]
                aliased_input_specs.remove(aliased_spec)
                measure_spec = aliased_spec.post_aggregation_spec
            else:
                measure_spec = instance.spec

            aliased_instances.append(
                MeasureInstance(
                    associated_columns=instance.associated_columns,
                    spec=measure_spec,
                    aggregation_state=instance.aggregation_state,
                    defined_from=instance.defined_from,
                )
            )

        return tuple(aliased_instances)

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        return InstanceSet(
            measure_instances=self._alias_measure_instances(instance_set.measure_instances),
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            identifier_instances=instance_set.identifier_instances,
            metric_instances=instance_set.metric_instances,
        )


class AddMetrics(InstanceSetTransform[InstanceSet]):
    """Adds the given metric instances to the instance set"""

    def __init__(self, metric_instances: List[MetricInstance]) -> None:  # noqa: D
        self._metric_instances = metric_instances

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        return InstanceSet(
            measure_instances=instance_set.measure_instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            identifier_instances=instance_set.identifier_instances,
            metric_instances=instance_set.metric_instances + tuple(self._metric_instances),
        )


class RemoveMeasures(InstanceSetTransform[InstanceSet]):
    """Remove measures from the instance set."""

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        return InstanceSet(
            measure_instances=(),
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            identifier_instances=instance_set.identifier_instances,
            metric_instances=instance_set.metric_instances,
        )


class RemoveMetrics(InstanceSetTransform[InstanceSet]):
    """Remove metrics from the instance set."""

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        return InstanceSet(
            measure_instances=instance_set.measure_instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            identifier_instances=instance_set.identifier_instances,
            metric_instances=(),
        )


class ChangeAssociatedColumns(InstanceSetTransform[InstanceSet]):
    """Change the columns associated with instances to the one specified by the resolver."""

    def __init__(self, column_association_resolver: ColumnAssociationResolver) -> None:  # noqa: D
        self._column_association_resolver = column_association_resolver

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D
        output_measure_instances = []
        for input_measure_instance in instance_set.measure_instances:
            output_measure_instances.append(
                MeasureInstance(
                    associated_columns=(
                        self._column_association_resolver.resolve_measure_spec(
                            measure_spec=input_measure_instance.spec
                        ),
                    ),
                    spec=input_measure_instance.spec,
                    defined_from=input_measure_instance.defined_from,
                    aggregation_state=input_measure_instance.aggregation_state,
                )
            )

        output_dimension_instances = []
        for input_dimension_instance in instance_set.dimension_instances:
            output_dimension_instances.append(
                DimensionInstance(
                    associated_columns=(
                        self._column_association_resolver.resolve_dimension_spec(
                            dimension_spec=input_dimension_instance.spec
                        ),
                    ),
                    spec=input_dimension_instance.spec,
                    defined_from=input_dimension_instance.defined_from,
                )
            )

        output_time_dimension_instances = []
        for input_time_dimension_instance in instance_set.time_dimension_instances:
            output_time_dimension_instances.append(
                TimeDimensionInstance(
                    associated_columns=(
                        self._column_association_resolver.resolve_time_dimension_spec(
                            time_dimension_spec=input_time_dimension_instance.spec
                        ),
                    ),
                    spec=input_time_dimension_instance.spec,
                    defined_from=input_time_dimension_instance.defined_from,
                )
            )

        output_identifier_instances = []
        for input_identifier_instance in instance_set.identifier_instances:
            output_identifier_instances.append(
                IdentifierInstance(
                    associated_columns=self._column_association_resolver.resolve_identifier_spec(
                        identifier_spec=input_identifier_instance.spec
                    ),
                    spec=input_identifier_instance.spec,
                    defined_from=input_identifier_instance.defined_from,
                )
            )

        output_metric_instances = []
        for input_metric_instance in instance_set.metric_instances:
            output_metric_instances.append(
                MetricInstance(
                    associated_columns=(
                        self._column_association_resolver.resolve_metric_spec(metric_spec=input_metric_instance.spec),
                    ),
                    spec=input_metric_instance.spec,
                    defined_from=input_metric_instance.defined_from,
                )
            )

        return InstanceSet(
            measure_instances=tuple(output_measure_instances),
            dimension_instances=tuple(output_dimension_instances),
            time_dimension_instances=tuple(output_time_dimension_instances),
            identifier_instances=tuple(output_identifier_instances),
            metric_instances=tuple(output_metric_instances),
        )


def create_select_columns_for_instance_sets(
    column_resolver: ColumnAssociationResolver,
    table_alias_to_instance_set: OrderedDict[str, InstanceSet],
) -> Tuple[SqlSelectColumn, ...]:
    """Creates select columns for instance sets coming from multiple table as defined in table_alias_to_instance_set.

    Used in cases where you join multiple tables and need to render select columns to access all of those.
    """
    column_set = SelectColumnSet()
    for table_alias, instance_set in table_alias_to_instance_set.items():
        column_set = column_set.merge(
            instance_set.transform(
                CreateSelectColumnsForInstances(
                    table_alias=table_alias,
                    column_resolver=column_resolver,
                )
            )
        )

    return column_set.as_tuple()
