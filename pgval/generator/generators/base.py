"""Contract for SST workload generators: ordered (event_time_ms, page) pairs."""
from dataclasses import dataclass
from typing import Iterable, Protocol


@dataclass(frozen=True)
class Config:
    seed: int = 42
    windows: int = 2
    pages: int = 2
    events_per_window: int = 10
    window_ms: int = 60000

    def validate(self):
        for name in ('windows', 'pages', 'events_per_window'):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f'{name} must be a positive integer')
        if type(self.seed) is not int:
            raise ValueError('seed must be an integer')
        if type(self.window_ms) is not int or self.window_ms != 60000:
            raise ValueError('Pinned SST baseline only supports window_ms=60000')
        if self.windows > 10000 or self.pages > 10000 or self.windows * self.events_per_window > 100000:
            raise ValueError('Small-test limits: <=10000 windows/pages and <=100000 data events')


class WorkloadGenerator(Protocol):
    name: str
    version: int
    description: str

    def events(self, config: Config) -> Iterable[tuple[int, str]]:
        """Yield only data events, ordered by time. Shared builder adds IDs and tail."""
        ...
