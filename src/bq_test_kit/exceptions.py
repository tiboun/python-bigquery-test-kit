# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

"""
    Define all exceptions related BQ-test-kit
"""

from typing import List


class RequirementsException(Exception):
    """
    Raised when requirements are not met.
    """


class ProjectNotDefinedException(Exception):
    """
    Raised when project is not defined in BigQueryTestKitConfig.
    """
    def __init__(self, project_name: str) -> None:
        super().__init__(f"Project {project_name} is not configured in BigQueryTestKitConfig.")


class InvalidInstanceException(Exception):
    """
    Raised when a given instance is not expected.
    """
    def __init__(self, given_instance: type,
                 *, expected_list_instances: List[type] = None,
                 expected_instances: List[type] = None) -> None:
        instances = ", ".join(
            [t.__name__ for t in expected_instances or []] +
            [f"List[{t.__name__}]" for t in expected_list_instances or []]
        )
        super().__init__(f"Expected one of {instances}"
                         f" but given {given_instance}")


class DataLiteralTransformException(Exception):
    """
        Raised when data could not be transformed to a data literal.
        May be caused by schema mismatch.
    """


class RowParsingException(Exception):
    """
        Raised when a line could not be parsed.
    """


class UnexpectedTypeException(Exception):
    """
        Raised when a BigQuery type is not handled.
    """
    def __init__(self, field_type: str) -> None:
        super().__init__(f"Type {field_type} is not handled.")
