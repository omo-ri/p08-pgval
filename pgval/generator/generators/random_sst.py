import random
from .base import Config


class RandomSST:
    name = 'sst-random'
    version = 1
    description = 'Random pages and event times, including window boundaries; seed-dependent.'

    def events(self, config: Config):
        rng = random.Random(config.seed)
        for window in range(config.windows):
            offsets = [0]
            if config.events_per_window >= 2:
                offsets.append(config.window_ms - 1)
            offsets.extend(rng.randrange(config.window_ms)
                           for _ in range(config.events_per_window - len(offsets)))
            for offset in sorted(offsets):
                yield window * config.window_ms + offset, f'/page-{rng.randrange(config.pages) + 1}'
