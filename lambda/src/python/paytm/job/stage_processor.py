import abc
from typing import List, Optional, Tuple

from paytm.job.job import Stage, Status


class StageProcessor(abc.ABC):

    @property
    @abc.abstractmethod
    def stage_type(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def max_concurrent_tasks(self) -> int:
        pass

    @property
    @abc.abstractmethod
    def max_attempts(self) -> int:
        pass

    @abc.abstractmethod
    def run(self, finished_stages: List[Stage], stage: Stage) -> Optional[str]:
        """
        Runs the give stage, optionally using results of input_stages.

        :param input_stages: results from previous stages can be used as inputs for this stage.
        :param stage: stage to run.
        :return: execution id or None if skipped
        """
        pass

    @abc.abstractmethod
    def poll(self, stage: Stage) -> Tuple[Status, Optional[dict]]:
        """
        Polls the underlying service to get updates for a given stage.

        :param stage: stage to check for updates.
        :return: a tuple of stage status and result
        """
        pass
