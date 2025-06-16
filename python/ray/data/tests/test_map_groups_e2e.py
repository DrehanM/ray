from typing import Optional

import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_map_groups_with_gpus(
    shutdown_only, configure_shuffle_method, disable_fallback_to_object_extension
):
    ray.shutdown()
    ray.init(num_gpus=1)

    rows = (
        ray.data.range(1).groupby("id").map_groups(lambda x: x, num_gpus=1).take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    class Identity:
        def __call__(self, batch):
            return batch

    rows = (
        ray.data.range(1).groupby("id").map_groups(Identity, concurrency=1).take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors_and_args(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    class Fn:
        def __init__(self, x: int, y: Optional[int] = None):
            self.x = x
            self.y = y

        def __call__(self, batch, q: int, r: Optional[int] = None):
            return {"x": [self.x], "y": [self.y], "q": [q], "r": [r]}

    rows = (
        ray.data.range(1)
        .groupby("id")
        .map_groups(
            Fn,
            concurrency=1,
            fn_constructor_args=[0],
            fn_constructor_kwargs={"y": 1},
            fn_args=[2],
            fn_kwargs={"r": 3},
        )
        .take_all()
    )

    assert rows == [{"x": 0, "y": 1, "q": 2, "r": 3}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
