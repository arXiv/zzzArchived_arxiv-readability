import os

from celery import states
from celery.result import AsyncResult
from django.conf import settings
from django.db import models

from .tasks import run_engrafo_task
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RenderError(Exception):
    pass


class RenderAlreadyStartedError(RenderError):
    """Render started when render has already been started."""


class RenderQuerySet(models.QuerySet):
    def running(self):
        return self.filter(state=Render.STATE_RUNNING)

    def succeeded(self):
        return self.filter(state=Render.STATE_SUCCESS)

    def failed(self):
        return self.filter(state=Render.STATE_FAILURE)


class Render(models.Model):
    # Convenience aliases so you don't have to import celery.states
    STATE_PENDING = states.PENDING
    STATE_STARTED = states.STARTED
    STATE_RETRY = states.RETRY
    STATE_SUCCESS = states.SUCCESS
    STATE_FAILURE = states.FAILURE

    SOURCE_TYPE_ARXIV = 'arxiv'
    SOURCE_TYPE_SUBMISSION = 'submission'

    source_type = models.CharField(max_length=20, choices=(
        (SOURCE_TYPE_ARXIV, 'arXiv'),
        (SOURCE_TYPE_SUBMISSION, 'Submission'),
    ))
    source_id = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    task_id = models.CharField(max_length=255, unique=True, null=True)
    state = models.CharField(max_length=20, default=states.PENDING, choices=(
        (states.PENDING, 'Pending'),
        (states.STARTED, 'Started'),
        (states.RETRY, 'Retrying'),
        (states.SUCCESS, 'Success'),
        (states.FAILURE, 'Failure'),
    ))
    logs = models.TextField(null=True, blank=True)

    objects = RenderQuerySet.as_manager()

    class Meta:
        get_latest_by = 'created_at'

    def __str__(self):
        return self.id

    def get_source_url(self):
        """
        Gets the source URL for this render, based on the source_type and source_id.
        """
        if self.source_type == "arxiv":
            return settings.ARXIV_SOURCE_URL_FORMAT.format(source_id=self.source_id)
        elif self.source_type == "submission":
            return f"http://fm-service-endpoint/upload/{self.source_id}/content"
        raise RenderError(f"Unknown source_type: {self.source_type}")

    def get_output_path(self):
        """
        Path to the directory that this render is in.

        If using local filesystem, return absolute path. This makes tests
        run in temporary directory correctly.
        """
        path = os.path.join("render-output", str(self.id))
        if settings.MEDIA_ROOT:
            path = os.path.join(settings.MEDIA_ROOT, path)
        return path

    def get_html_path(self):
        """
        Path to the HTML file of this render.
        """
        return os.path.join(self.get_output_path(), "index.html")

    def get_output_url(self):
        """
        Returns the URL to the output path.
        """
        if self.state != Render.STATE_SUCCESS:
            return None
        return os.path.join(settings.MEDIA_URL, "render-output", str(self.id))

    def get_task_result(self):
        """
        Creates a task result object from task_id. Returns None if this render
        has not been delayed yet.
        """
        if not self.task_id:
            return None
        return AsyncResult(self.task_id)

    def update_state(self):
        """
        Update the state from the Celery result.
        """
        result = self.get_task_result()
        if not result:
            return

        if result.state == states.SUCCESS:
            # Celery provides no simple way of passing back data with a
            # failure, so instead use the error code to infer the final state.
            if result.result["exit_code"] == 0:
                logger.debug('got exit_code 0')
                self.state = states.SUCCESS
            else:
                logger.debug('not non-zero exit code')
                self.state = states.FAILURE

            self.logs = result.result.get("logs")
            logger.debug('got logs: %s', self.logs)
        else:
            logger.debug('task not successful')
            self.state = result.state

        self.save(update_fields=["state", "logs"])

    def delay(self):
        """
        Delay the Celery task for this render and set task_id. This method will
        save the model with the task_id.
        """
        result = run_engrafo_task.delay(self.get_source_url(), self.get_output_path())
        self.task_id = result.task_id
        self.save(update_fields=["task_id"])
