# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from collections import OrderedDict
from contextlib import ExitStack
from typing import List, NoReturn, Tuple

from logzero import logger

from bq_test_kit.bq_dsl.bq_resources.base_bq_resource import BaseBQResource
from bq_test_kit.bq_dsl.bq_resources.dataset import Dataset
from bq_test_kit.bq_dsl.bq_resources.project import Project
from bq_test_kit.bq_dsl.bq_resources.table import Table
from bq_test_kit.exceptions import InvalidInstanceException


class Tables:
    """Resource manager of BQResource that allows to create all resources underneath it and return a tuple of tables
       in order to potentially load data.

       Tables are in the order of the recursive path, from left to right, top to bottom.

       Given a project with two datasets D1 and D2 and both of them have two table, D1.T1, D1.T2, D2.T3 and D2.T4,
       Resource creation will be : D1, D2, D1.T1, D1.T2, D2.T3 and D2.T4.

       Tuples will be T1, T2, T3, T4.

       This Resource manager ease sharing of datasets and tables definitions with fixtures.
    """

    def __init__(self, *bq_resources: BaseBQResource) -> None:
        """List of BaseBQResource to create.
        """
        self.bq_resources = bq_resources
        self._close = None

    @staticmethod
    def from_(*bq_resources: BaseBQResource):
        """Same as constructor but provides a more natural way to instantiate.

        Returns:
            Tables: resource manager.
        """
        return Tables(*bq_resources)

    def __enter__(self) -> Tuple[Table, ...]:
        flattened_bq_resources = self._flatten_bq_resources()
        bqr_str = ", ".join([bqr.fqdn() for bqr in flattened_bq_resources])
        logger.info("Creating the following resources %s", bqr_str)
        with ExitStack() as stack:

            def stack_and_append_table(bq_resource):
                stack.enter_context(bq_resource)
                return bq_resource
            all_managed_resources = [stack_and_append_table(bqr) for bqr in flattened_bq_resources]
            managed_tables = filter(lambda r: isinstance(r, Table), all_managed_resources)
            self._close = stack.pop_all().close
            return tuple(managed_tables)

    def __exit__(self, exception_type, exception_value, traceback) -> NoReturn:
        if self._close:
            self._close()
            self._close = None

    def _flatten_bq_resources(self) -> List[BaseBQResource]:

        def _flatten(bq_resource: BaseBQResource) -> List[BaseBQResource]:
            res = []
            if isinstance(bq_resource, Project):
                res = [bq_resource] + bq_resource.datasets + [t for d in bq_resource.datasets for t in d.tables]
            elif isinstance(bq_resource, Dataset):
                res = [bq_resource] + bq_resource.tables
            elif isinstance(bq_resource, Table):
                res = [bq_resource]
            else:
                raise InvalidInstanceException(type(bq_resource), expected_instances=[Project, Dataset, Table])
            return res
        return OrderedDict((flattened_bqr.fqdn(), flattened_bqr)
                           for bq_resource in self.bq_resources
                           for flattened_bqr in _flatten(bq_resource)
                           ).values()
