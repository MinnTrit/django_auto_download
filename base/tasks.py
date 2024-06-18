from celery import shared_task
from Scrapper import Scrapper
from django_rq import job
from huey import RedisHuey, crontab
from django.conf import settings
from huey.contrib.djhuey import task

huey = RedisHuey('myhuey', host='127.0.0.1', port=6379)

# Used to initialize the Huey task
@task()
def run_scrapper(start_date, end_date, seller_ids_list, launcher):
    scrapper = Scrapper(start_date, end_date, seller_ids_list, launcher)
    scrapper.initialize()
    scrapper.run()
