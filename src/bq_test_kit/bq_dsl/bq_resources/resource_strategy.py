# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

"""
    This module contains all kind of resource strategy.
"""

from typing import Callable


class BaseResourceStrategy():
    """Resource management strategy to use with BaseBQResource.
    """

    def before(self, delete: Callable[[], None], create: Callable[[], None]) -> None:
        """Apply management strategy on bq_resource before it's usage.

        Args:
            delete (BaseBQResource): bq_resource to manage.

        Raises:
            NotImplementedError: All resource strategy must implement this method
        """
        raise NotImplementedError("Resource strategy must implement before method")

    def after(self, delete: Callable[[], None]) -> None:
        """Apply management strategy on bq_resource after it's usage.

        Args:
            bq_resource (BaseBQResource): bq_resource to manage.

        Raises:
            NotImplementedError: All resource strategy must implement this method
        """
        raise NotImplementedError("Resource strategy must implement after method")


class Noop(BaseResourceStrategy):
    """Doesn't manage the resource at all.
    """
    def before(self, delete: Callable[[], None], create: Callable[[], None]) -> None:
        pass

    def after(self, delete: Callable[[], None]):
        pass


class CleanBeforeAndAfter(BaseResourceStrategy):
    """Clean before the creation of the resource and after its usage.
    """
    def before(self, delete: Callable[[], None], create: Callable[[], None]) -> None:
        delete()
        create()

    def after(self, delete: Callable[[], None]):
        delete()


class CleanAfter(BaseResourceStrategy):
    """Create and destroy after its usage.
    """
    def before(self, delete: Callable[[], None], create: Callable[[], None]) -> None:
        create()

    def after(self, delete: Callable[[], None]):
        delete()


class CleanBeforeAndKeepAfter(BaseResourceStrategy):
    """Clean before the creation of the resource and let instance after it's usage.
    """
    def before(self, delete: Callable[[], None], create: Callable[[], None]) -> None:
        delete()
        create()

    def after(self, delete: Callable[[], None]):
        pass
