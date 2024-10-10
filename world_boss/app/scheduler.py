from apscheduler.schedulers.background import BackgroundScheduler

from world_boss.app.config import config
from world_boss.app.tasks import check_season

scheduler = BackgroundScheduler()


def check():
    check_season.delay()


scheduler.add_job(check, "interval", seconds=config.scheduler_interval)
