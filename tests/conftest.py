import os

import pytest

PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PYTEST_DIR)

os.path.join(os.path.join(ROOT_DIR, "orco"))

from orco.globals import reset_registered_computations  # noqa
from orco import Runtime  # noqa


@pytest.fixture(scope="function")
def init_orco():
    reset_registered_computations()


@pytest.fixture()
def runtime():
    return Runtime("sqlite:///:memory:")
