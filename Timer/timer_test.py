from datetime import datetime as dt


def time_format(dt_object):
    """
    Converts time-difference into a formatted string
    :param dt_object: time difference (datetime.delta) object to format
    :return: dictionary formatted with time components of dt_object
    """
    diff_days = dt_object.days
    diff_seconds = dt_object.seconds
    minutes, sec_remainder = diff_seconds // 60, diff_seconds % 60
    hours, min_remainder = 0, 0
    if minutes >= 60:
        hours, min_remainder = minutes // 60, minutes % 60
    return {
        "days": diff_days,
        "hours": hours,
        # "minutes": minutes if not min_remainder else min_remainder,
        "minutes": min_remainder,
        "seconds": sec_remainder
    }


if __name__ == '__main__':
    target_date = dt(year=2020, month=2, day=13)
    time_diff = target_date - dt.now()
    print("{}".format(dt.now().replace(microsecond=0)))
    print(time_format(time_diff))
