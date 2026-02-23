from app.tasks.celery_app import celery_app


if __name__ == "__main__":
    # Windows 下默认 prefork 不可用，使用 solo 进程池更稳定。
    celery_app.worker_main(["worker", "--loglevel=info", "--pool=solo"])
