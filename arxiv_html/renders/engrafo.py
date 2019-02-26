import os
from django.conf import settings
import docker
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def run_engrafo(source_url, output_path):
    environment = {}
    volumes = {}
    # Production
    if settings.MEDIA_USE_S3:
        output_path = f"s3://{settings.AWS_STORAGE_BUCKET_NAME}/{output_path}"
        environment["AWS_ACCESS_KEY_ID"] = settings.AWS_ACCESS_KEY_ID
        environment["AWS_SECRET_ACCESS_KEY"] = settings.AWS_SECRET_ACCESS_KEY
        environment["AWS_S3_REGION_NAME"] = settings.AWS_S3_REGION_NAME
    # Development
    else:
        # Mount code like other development containers. This is primarily
        # so MEDIA_ROOT works as the output directory.
        # HOST_PWD is set in docker-compose.yml
        volumes[os.environ["HOST_PWD"]] = {"bind": "/code", "mode": "rw"}
    logger.debug("run engragfo on %s and put to %s", source_url, output_path)
    client = docker.from_env()

    container = client.containers.run(
        settings.ENGRAFO_IMAGE,
        ["engrafo", source_url, output_path],
        environment=environment,
        volumes=volumes,
        detach=True,
    )
    exit_code = container.wait()["StatusCode"]
    logger.debug('got exit code: %i', exit_code)
    logger.debug('got logs: %s:', container.logs().decode("utf-8"))
    return {"exit_code": exit_code, "logs": container.logs().decode("utf-8")}
