import re
import datetime
from datetime import timezone

MONTHS = {
    'jan': 1,
    'feb': 2,
    'mar': 3,
    'apr': 4,
    'may': 5,
    'jun': 6,
    'jul': 7,
    'aug': 8,
    'sep': 9,
    'oct': 10,
    'nov': 11,
    'dec': 12
}

def parse_time(line, now=None):
    # Regex to read Unix, NetWare and NetPresenz time format 
    unix_re = re.compile("^([A-Za-z]{3})" # month
        "\\s+(\\d{1,2})" # day of month
        "\\s+([:\\d]{4,5})$" # time of day or year
    )

    match = unix_re.fullmatch(line)
    if match:
        month = match[1]
        day = match[2]
        year = match[3]

        month = MONTHS.get(month.lower())
        day = int(day)

        yr_match = re.fullmatch("(\\d{2}):(\\d{2})", year)
        if yr_match:
            hour = int(yr_match[1])
            minute = int(yr_match[2])

            now = now or datetime.datetime.utcnow()

            if (now.month < month) or ((now.month == month) and (now.day < day)):
                year = now.year - 1
            else:
                year = now.year
        else:
            hour = 0
            minute = 0
            year = int(year)

        return datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=0, tzinfo=timezone.utc)

    # Regex to read MultiNet time format 
    multinet_re = re.compile("^(\\d{1,2})" # day of month
        "-([A-Za-z]{3})" # month
        "-(\\d{4})" # year
        "\\s+(\\d{2})" # hour
        ":(\\d{2})" # minute
        "(:(\\d{2}))?$" # second
    )

    match = multinet_re.fullmatch(line)
    if match:
        day = int(match[1])
        month = match[2]
        year = int(match[3])
        hour = int(match[4])
        minute = int(match[5])
        second = match[7]
        if second:
            second = int(second)
        else:
            second = 0

        month = MONTHS.get(month.lower())

        return datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second, tzinfo=timezone.utc)

    # Regex to read MSDOS time format 
    msdos_re = re.compile("^(\\d{2})" # month
        "-(\\d{2})" # day of month
        "-(\\d{2})" # year
        "\\s+(\\d{2})" # hour
        ":(\\d{2})" # minute
        "([AP]M)$" # AM or PM
    )

    match = msdos_re.fullmatch(line)
    if match:
        month = int(match[1])
        day = int(match[2])
        year = int(match[3])
        hour = int(match[4])
        minute = int(match[5])
        am_pm = match[6]

        if year < 70:
            year += 2000
        else:
            year += 1900

        if hour == 12:
            hour -= 12
        if am_pm == "PM":
            hour += 12

        return datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=0, tzinfo=timezone.utc)