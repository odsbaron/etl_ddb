from abc import ABC, abstractmethod
from typing import Iterator, Tuple
import pandas as pd


class BaseReader(ABC):
    @abstractmethod
    def read_batches(self) -> Iterator[Tuple[pd.DataFrame, str]]:
        ...

    @abstractmethod
    def get_checkpoint(self) -> dict:
        ...

    @abstractmethod
    def close(self):
        ...
