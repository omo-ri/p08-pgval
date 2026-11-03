"""Explicit registry: adding a workload does not require changing CLI or plan builder."""
from .base import Config, WorkloadGenerator
from .random_sst import RandomSST
from .uniform_sst import UniformSST

GENERATORS: dict[str, WorkloadGenerator] = {
    generator.name: generator for generator in (RandomSST(), UniformSST())
}


def get_generator(name: str) -> WorkloadGenerator:
    try:
        return GENERATORS[name]
    except KeyError:
        raise ValueError(f'Unknown generator {name!r}; available: {", ".join(GENERATORS)}') from None
