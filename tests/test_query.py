# -*- coding: utf-8 -*-
import datetime as dt
import pytest

from doorman.models import (
    DistributedQuery,
    DistributedQueryResult,
    DistributedQueryTask,
    Node,
    ResultLog,
)
from doorman.query import (
    Filter,
    FilterType,
    QueryType,
    get_results,
)

from .factories import NodeFactory, PackFactory, QueryFactory, TagFactory


@pytest.mark.usefixtures('db')
class TestQueryResultLog:

    def test_filter_node(self):
        node1 = NodeFactory(host_identifier='node1')
        node1.save()
        node2 = NodeFactory(host_identifier='node2')
        node2.save()

        rs1 = ResultLog.create(
            name='query1',
            action='added',
            columns={},
            node=node1,
        )
        rs2 = ResultLog.create(
            name='query1',
            action='added',
            columns={},
            node=node2,
        )

        results = get_results(
            query_type=QueryType.RESULT,
            filters=[
                Filter(type=FilterType.NODE, value=node1.id),
            ]
        )

        assert results.items == [rs1]

    def test_filter_action(self):
        node = NodeFactory(host_identifier='node')
        node.save()

        rs1 = ResultLog.create(
            name='query1',
            action='added',
            columns={},
            node=node,
        )
        rs2 = ResultLog.create(
            name='query1',
            action='removed',
            columns={},
            node=node,
        )

        results = get_results(
            query_type=QueryType.RESULT,
            filters=[
                Filter(type=FilterType.ACTION, value='added'),
            ]
        )

        assert results.items == [rs1]

    def test_filter_query(self):
        node = NodeFactory(host_identifier='node')
        node.save()

        rs1 = ResultLog.create(
            name='query1',
            action='added',
            columns={},
            node=node,
        )
        rs2 = ResultLog.create(
            name='query2',
            action='added',
            columns={},
            node=node,
        )

        results = get_results(
            query_type=QueryType.RESULT,
            filters=[
                Filter(type=FilterType.QUERY, value=rs2.id),
            ]
        )

        assert results.items == [rs2]


@pytest.mark.usefixtures('db')
class TestQueryDistributed:

    def test_filter_node(self):
        node1 = NodeFactory(host_identifier='node1')
        node1.save()
        node2 = NodeFactory(host_identifier='node2')
        node2.save()

        # Create a single dummy query.
        q = DistributedQuery.create(
            sql='SELECT 1, 2, 3;',
        )

        # Create a task for each node.
        t1 = DistributedQueryTask.create(
            node=node1,
            distributed_query=q,
        )
        t2 = DistributedQueryTask.create(
            node=node2,
            distributed_query=q,
        )

        for x in (t1, t2):
            x.status = DistributedQueryTask.COMPLETE
            x.save()

        # And now create result logs for each task.
        t1r1 = DistributedQueryResult.create(
            columns={'result': 1},
            distributed_query_task=t1,
            distributed_query=q,
        )
        t1r2 = DistributedQueryResult.create(
            columns={'result': 2},
            distributed_query_task=t1,
            distributed_query=q,
        )
        t2r1 = DistributedQueryResult.create(
            columns={'result': 1},
            distributed_query_task=t2,
            distributed_query=q,
        )
        t2r1 = DistributedQueryResult.create(
            columns={'result': 2},
            distributed_query_task=t2,
            distributed_query=q,
        )

        # Finally, run our query
        results = get_results(
            query_type=QueryType.DISTRIBUTED,
            filters=[
                Filter(type=FilterType.NODE, value=node1.id),
            ]
        )

        # Task one is for node 1, so we should only have these results.
        assert results.items == [t1r1, t1r2]

    def test_filter_query(self):
        node = NodeFactory(host_identifier='node')
        node.save()

        # Create two queries
        q1 = DistributedQuery.create(sql='SELECT 1;')
        q2 = DistributedQuery.create(sql='SELECT 2;')

        # Create a task for each query.
        t1 = DistributedQueryTask.create(
            node=node,
            distributed_query=q1,
        )
        t2 = DistributedQueryTask.create(
            node=node,
            distributed_query=q2,
        )

        for x in (t1, t2):
            x.status = DistributedQueryTask.COMPLETE
            x.save()

        # And now create result logs for each task.
        t1r = DistributedQueryResult.create(
            columns={'result': 1},
            distributed_query_task=t1,
            distributed_query=q1,
        )
        t2r = DistributedQueryResult.create(
            columns={'result': 2},
            distributed_query_task=t2,
            distributed_query=q2,
        )

        # Finally, run our query
        results = get_results(
            query_type=QueryType.DISTRIBUTED,
            filters=[
                Filter(type=FilterType.QUERY, value=q1.id),
            ]
        )

        # Should only have result(s) from the first query/task
        assert results.items == [t1r]

    def test_filter_status(self):
        node = NodeFactory(host_identifier='node')
        node.save()

        # Create two queries
        q1 = DistributedQuery.create(sql='SELECT 1;')
        q2 = DistributedQuery.create(sql='SELECT 2;')

        # Create a task for each query.
        t1 = DistributedQueryTask.create(
            node=node,
            distributed_query=q1,
        )
        t2 = DistributedQueryTask.create(
            node=node,
            distributed_query=q2,
        )

        t1.status = DistributedQueryTask.COMPLETE
        t1.save()

        # And now create result logs for each task.
        t1r = DistributedQueryResult.create(
            columns={'result': 1},
            distributed_query_task=t1,
            distributed_query=q1,
        )
        t2r = DistributedQueryResult.create(
            columns={'result': 2},
            distributed_query_task=t2,
            distributed_query=q2,
        )

        # Finally, run our query
        results = get_results(
            query_type=QueryType.DISTRIBUTED,
            filters=[
                Filter(type=FilterType.STATUS, value=DistributedQueryTask.COMPLETE),
            ]
        )

        # Should only have result(s) from the complete task
        assert results.items == [t1r]
