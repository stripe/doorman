# -*- coding: utf-8 -*-
from collections import namedtuple

from enum import Enum, unique

from doorman.models import (
    DistributedQuery,
    DistributedQueryResult,
    DistributedQueryTask,
    Node,
    ResultLog,
)


@unique
class QueryType(Enum):
    """
    Enum that specifies what sort of result logs we're querying for.
    """
    RESULT = 1
    DISTRIBUTED = 2


@unique
class FilterType(Enum):
    """
    Enum that specifies what type of filter we're applying to the result logs.
    """
    # General filter options
    NODE = 1
    QUERY = 2
    TIMESTAMP = 3

    # Only for ResultLog
    ACTION = 4

    # Only for DistributedQuery
    STATUS = 5


Filter = namedtuple('Filter', ['type', 'value'])


def get_results(query_type, filters=None, page=1, per_page=20, order_by=None, sort='asc'):
    """
    Given a query type and an optional set of filters and aggregation options,
    will return a set of results for display.
    """
    if filters is None:
        filters = []

    # Get the base query
    if query_type == QueryType.RESULT:
        query = get_resultlog_results(filters, page, per_page)
    elif query_type == QueryType.DISTRIBUTED:
        query = get_distributed_results(filters, page, per_page)
    else:
        raise ValueError('Unknown query type: {0}'.format(query_type))

    if order_by is not None:
        #order_by = getattr(
        pass

    return query.paginate(page=page, per_page=per_page)


def get_resultlog_results(filters, page, per_page):
    query = ResultLog.query

    for filter in filters:
        if filter.type == FilterType.NODE:
            node = Node.query.filter(Node.id == filter.value).one()
            query = query.filter(ResultLog.node == node)

        elif filter.type == FilterType.QUERY:
            query = query.filter(ResultLog.id == filter.value)

        elif filter.type == FilterType.TIMESTAMP:
            # TODO
            raise NotImplementedError

        elif filter.type == FilterType.ACTION:
            query = query.filter(ResultLog.action == filter.value)

        else:
            raise ValueError('Unknown filter type for ResultLog: {0}'.format(filter))

    return query


def get_distributed_results(filters, page, per_page):
    query = DistributedQueryTask.query

    for filter in filters:
        if filter.type == FilterType.NODE:
            node = Node.query.filter(Node.id == filter.value).one()
            query = query.filter(DistributedQueryTask.node == node)

        elif filter.type == FilterType.QUERY:
            dq = DistributedQuery.query.filter(DistributedQuery.id == filter.value).one()
            query = query.filter(DistributedQueryTask.distributed_query == dq)

        elif filter.type == FilterType.TIMESTAMP:
            # TODO
            raise NotImplementedError

        elif filter.type == FilterType.STATUS:
            query = query.filter(DistributedQueryTask.status == filter.value)

        else:
            raise ValueError('Unknown filter type for DistributedQuery: {0}'.format(filter))

    # The above query will return a list of tasks that we're interested in.  We
    # now want to load all results that match this list of tasks.  We can't
    # (yet) use `IN` for relationships, so we need to get all the task IDs and
    # use those instead.
    ids = [x.id for x in query.all()]
    results = DistributedQueryResult.query.filter(DistributedQueryResult.distributed_query_task_id.in_(ids))
    return results
