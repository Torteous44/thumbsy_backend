from celery import Celery

celery_app = Celery(
    'thumbsy_tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    include=['services.ingestion.app.scheduler.tasks']
)

# Optional configurations
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# This is important - it exposes the Celery app instance
celery = celery_app 