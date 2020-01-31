# from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime as dt
from Timer.timer_test import time_format

sched = BackgroundScheduler()


@sched.scheduled_job('interval', seconds=1)
def timer():
    target_date = dt(year=2020, month=2, day=13)
    time_diff = target_date - dt.now()
    print("{}".format(dt.now().replace(microsecond=0)))
    components = time_format(time_diff)
    msg = "Time left: {} days : {} h : {} m : {} s".format(
        components["days"],
        components["hours"],
        components["minutes"],
        components["seconds"])
    print(msg)
    return msg


sched.start()
