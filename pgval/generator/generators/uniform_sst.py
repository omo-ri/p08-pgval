from .base import Config


class UniformSST:
    name = 'sst-uniform'
    version = 1
    description = 'Evenly spaced times within each window; round-robin pages; seed-independent.'

    def events(self, config: Config):
        for window in range(config.windows):
            for index in range(config.events_per_window):
                offset = index * (config.window_ms - 1) // max(1, config.events_per_window - 1)
                page = (window * config.events_per_window + index) % config.pages + 1
                yield window * config.window_ms + offset, f'/page-{page}'
