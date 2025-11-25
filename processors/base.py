from abc import ABC, abstractmethod
from database import ReasoningDatabase

class Processor(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def process(self, database: ReasoningDatabase):
        """
        Abstract method to perform work on the database.
        """
        pass
