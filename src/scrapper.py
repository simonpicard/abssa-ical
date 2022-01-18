#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Parse ABSSA's calendar and output to ics."""

import datetime
import json
import logging
import os
import re
import sys
import uuid

import pytz
import requests
from bs4 import BeautifulSoup
from icalendar import (
    Calendar,
    Event,
    Timezone,
    TimezoneDaylight,
    TimezoneStandard,
    vText,
)


def get_field_address(url):
    """Get the field address from a team's page."""
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    ul = soup.find_all("ul", {"class": "abssa_list"})
    locality = re.search("Localité:(.+)", ul[3].text).group(1).strip()
    street = re.search("Rue:(.+)", ul[3].text).group(1).strip()
    name = re.search("Situation:(.+)", ul[3].text).group(1).strip()
    if len(name) > 0:
        address = [name]
    else:
        address = []
    address.append(street)
    address.append(locality)

    address = ", ".join(address)

    car_access = ""
    for li in ul[3].find_all("li"):
        if li.span:
            if li.span.text == "Acces voiture":
                car_access = li.text.strip()

    return address, car_access


def parse_day(res, game_date, day_i):
    """Parse a game day JSON.

    Args:
        res (dict): the game day JSON
        game_date (str): Game day's date
        day_i (int): Game day's number
    """
    events = []
    for j in range(len(res)):
        division = res[j]["d"]
        games = res[j]["m"]
        for game in games:
            event = {}
            if ":" in game["ds"]:
                time = game["ds"]
                home_team = game["ci"]
                away_team = game["co"]
                home_url_name = game["uti"]
                home_id = game["mi"]
                home_url = (
                    f"http://www.abssa.org/team/{home_id}/{home_url_name}"
                )
                home_address, car_access = get_field_address(home_url)

                day, month, year = game_date.split("/")
                hour, minute = time.split(":")

                event[
                    "summary"
                ] = f"ABSSA {division} DAY {day_i}: {home_team} - {away_team}"
                event["location"] = home_address
                event["start"] = datetime.datetime(
                    int(year),
                    int(month),
                    int(day),
                    int(hour),
                    int(minute),
                    tzinfo=pytz.timezone("Europe/Brussels"),
                )
                event["end"] = event["start"] + datetime.timedelta(
                    minutes=35 * 2 + 15
                )
                event["description"] = car_access

                event["home_team"] = home_team
                event["away_team"] = away_team
                event["division"] = division
                events.append(event)
    return events


def get_tz():
    """Retruns a Europe/Brussels VTIMEZONE for ics."""
    tzc = Timezone()
    tzc.add("tzid", "Europe/Brussels")
    tzc.add("x-lic-location", "Europe/Brussels")

    tzs = TimezoneStandard()
    tzs.add("tzname", "CET")
    tzs.add("dtstart", datetime.datetime(1970, 10, 25, 3, 0, 0))
    tzs.add("rrule", {"freq": "yearly", "bymonth": 10, "byday": "-1su"})
    tzs.add("TZOFFSETFROM", datetime.timedelta(hours=2))
    tzs.add("TZOFFSETTO", datetime.timedelta(hours=1))

    tzd = TimezoneDaylight()
    tzd.add("tzname", "CEST")
    tzd.add("dtstart", datetime.datetime(1970, 3, 29, 2, 0, 0))
    tzs.add("rrule", {"freq": "yearly", "bymonth": 3, "byday": "-1su"})
    tzd.add("TZOFFSETFROM", datetime.timedelta(hours=1))
    tzd.add("TZOFFSETTO", datetime.timedelta(hours=2))

    tzc.add_component(tzs)
    tzc.add_component(tzd)
    return tzc


def generate_cal(events, team):
    """Generate and ics calendar based on an events dictionary.

    Args:
        events (dict): the events dictionary scrapped and parsed
        team (str): The name of the team to create the ics calendar for
    """
    cal = Calendar()
    cal["X-WR-CALNAME"] = f"ABSSA {team}"
    cal["PRODID"] = str(uuid.uuid4())
    cal["VERSION"] = "0.1"
    cal.add_component(get_tz())
    for event in events:
        if team in (event["home_team"], event["away_team"]):
            ev = Event()
            ev.add("DTSTAMP", datetime.datetime.now())
            ev["uid"] = str(uuid.uuid4())

            ev.add("dtstart", event["start"])
            ev.add("dtend", event["end"])

            ev["location"] = vText(event["location"])
            ev["summary"] = vText(event["summary"])
            ev["description"] = vText(event["description"])
            cal.add_component(ev)
    return cal


def url_safen(s):
    """Replace characters in an str to avoid illegal characters."""
    s = s.lower()
    s = s.replace(" ", "_")
    s = s.replace(".", "_")
    s = s.replace("'", "_")
    s = s.replace("-", "_")
    s = s.replace("(", "_")
    s = s.replace(")", "_")
    return s


def scrap_calendar(base_path):
    """Scrap and parse calendar from the ABSSA website and save it as ics."""
    url = "http://www.abssa.org/championnat"
    logging.info(f"Fetching data from {url}.")
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    ul = soup.find_all("ul", {"class": "dropdown-menu"})

    # get date of game days
    lis = ul[1].find_all("li")
    game_days = []
    game_days_value = []
    for i in range(len(lis)):
        li = lis[i]
        game_days.append(li.text)
        game_days_value.append(li.a["data-value"])

    logging.info(f"Found {len(game_days)} days to fetch.")

    # fetch and parse data for each day
    events = []
    for i in range(len(game_days)):
        logging.info(f"Scrapping day {game_days[i]}.")
        url = f"http://www.abssa.org/amatches/{game_days_value[i]}"
        res = requests.get(url)
        game_date = game_days[i]
        res = json.loads(res.text)

        day_event = parse_day(res, game_date, i + 1)
        logging.info(
            f"Found {len(day_event)} different fixtures for current day."
        )

        events.extend(day_event)

    # get divisions' and teams' names
    divisions = set()
    teams = {}
    for e in events:
        divisions.add(e["division"])

    for d in divisions:
        teams_div = set()
        for e in events:
            if d == e["division"]:
                teams_div.add(e["home_team"])
                teams_div.add(e["away_team"])
            teams[d] = teams_div

    logging.info(f"Writting ics output in {base_path}.")
    for d in divisions:
        for t in teams[d]:
            cal = generate_cal(events, t)
            t_safe = url_safen(t)

            cal_dir = os.path.join(base_path, str(d))
            cal_path = os.path.join(cal_dir, f"{t_safe}.ics")
            if not os.path.exists(cal_dir):
                os.makedirs(cal_dir)
            with open(cal_path, "wb") as f:
                f.write(cal.to_ical())


if __name__ == "__main__":
    assert (
        "-d" in sys.argv
    ), "Please specify an output directory with `-d`argument."
    logging.getLogger().setLevel(logging.INFO)
    base_path = sys.argv[sys.argv.index("-d") + 1]
    scrap_calendar(base_path)
