from abc import ABC, abstractmethod


class BaseFlow(ABC):
    @abstractmethod
    def execute_in_run(self):
        """Execute flow when inside of a run"""
        ...

    @abstractmethod
    def execute(self):
        """Execute flow starting a run"""
        ...

    @abstractmethod
    def log_run_details(self):
        """Log details about the run to the tracking server"""
        ...
