from abc import ABC, abstractmethod


class BaseWriter(ABC):
    @abstractmethod
    def write_batch(self, df, batch_id: str):
        ...

    @abstractmethod
    def close(self):
        ...
