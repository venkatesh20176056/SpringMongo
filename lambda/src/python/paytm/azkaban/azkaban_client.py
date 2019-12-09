from azkaban.remote import Session as AzkabanSession
import json
import logging


_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


class AzkabanClient:

    def __init__(self, url: str) -> None:
        """
        :param url: 'http://{user}:{pass}@{host}:8099'
        """
        self.url = url
        self._azkaban_session = AzkabanSession(self.url)

    def submit_job(self, azkaban_project: str, azkaban_flow: str, job_properties: dict) -> int:
        workflow = self._azkaban_session.run_workflow(
            azkaban_project,
            azkaban_flow,
            jobs=None,  # Default setting to run all jobs within the flow
            concurrent='pipeline:2',  # block job A until the previous flow job A's children finish
            properties=job_properties,
            on_failure='finish',
            notify_early=True
        )

        execution_id = int(workflow['execid'])
        job_json = json.dumps(job_properties)
        _log.info('Submitted job: {0} to azkaban'.format(job_json))

        return execution_id

    def get_status(self, execution_id: str) -> dict:
        return self._azkaban_session.get_execution_status(execution_id)
