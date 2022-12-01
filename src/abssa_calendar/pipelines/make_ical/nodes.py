"""
This is a boilerplate pipeline 'make_ical'
generated using Kedro 0.18.3
"""
import datetime
import uuid

import pytz
from icalendar import Calendar, Event, vCalAddress, vGeo, vText


def generate_ics(events_df, name, desc, calendar_id):
    events_df = events_df.sort_values("datetime_start")

    cal = Calendar()
    cal["VERSION"] = "2.0"
    cal["PRODID"] = calendar_id
    cal["X-WR-CALNAME"] = name
    cal["X-WR-CALDESC"] = desc
    cal["X-WR-TIMEZONE"] = "Europe/Brussels"

    for _, event in events_df.iterrows():
        ev = Event()
        ev.add("DTSTAMP", datetime.datetime.now())
        ev["uid"] = str(uuid.uuid4())
        ev.add(
            "DTSTART",
            event["datetime_start"].astimezone(pytz.UTC),
        )
        ev.add(
            "DTEND",
            event["datetime_end"].astimezone(pytz.UTC),
        )
        # ev["dtstart"] = (
        #     event["datetime_start"].astimezone(pytz.UTC).strftime("%Y%m%dT%H%M%SZ")
        # )
        # ev["dtend"] = (
        #     event["datetime_end"].astimezone(pytz.UTC).strftime("%Y%m%dT%H%M%SZ")
        # )
        ev["location"] = vText(event["address"])
        ev["geo"] = vGeo((event["latitude"], event["longitude"]))
        ev["summary"] = vText(event["ical_summary"])
        ev["description"] = vText(event["ical_desc"])
        ev["organizer"] = vCalAddress(f'MAILTO:{event["email_home"]}')
        cal.add_component(ev)
    return cal


def get_calendars(events_df, metadata_df):
    calendars_clubs = get_calendars_clubs(events_df, metadata_df)
    calendars_days = get_calendars_days(events_df)

    calendars = {**calendars_clubs, **calendars_days}

    return calendars


def get_calendars_clubs(events_df, metadata_df):
    calendars = {}
    for rid_, row in metadata_df.iterrows():
        name = row["calname"]
        desc = row["caldesc"]
        calendar_id = row["calendar_id"]

        scope_home = events_df["club_id_home"] == row["club_id"]
        scope_home &= events_df["team_id_home"] == row["team_id"]
        scope_away = events_df["club_id_away"] == row["club_id"]
        scope_away &= events_df["team_id_away"] == row["team_id"]
        scope = (scope_home) | (scope_away)

        scoped_event_df = events_df.loc[scope]
        calendars[calendar_id] = (
            generate_ics(scoped_event_df, name, desc, calendar_id)
            .to_ical()
            .decode("utf-8")
        )
    return calendars


def get_calendars_days(events_df):
    calendars = {}
    for day in events_df["day"].unique():
        for division in events_df["division"].unique():
            name = f"D{division} Journée {day}"
            desc = f"Les matches de la journée {day} en divion {division} d'ABSSA."
            calendar_id = f"abbsa_j_{day}_d_{division.lower()}"

            scope = events_df["day"] == day
            scope &= events_df["division"] == division

            scoped_event_df = events_df.loc[scope]
            calendars[calendar_id] = (
                generate_ics(scoped_event_df, name, desc, calendar_id)
                .to_ical()
                .decode("utf-8")
            )
    return calendars


def get_metadata_json(metata_df):
    metadata_json = metata_df.set_index("calendar_id").to_dict("index")
    return metadata_json


def get_metadata_day_div_json(events_df):
    metadata_day_div_json = events_df.groupby(["day", "division"], as_index=False)[
        "datetime_start"
    ].min()
    metadata_day_div_json["calendar_id"] = metadata_day_div_json.apply(
        lambda x: f"abbsa_j_{x['day']}_d_{x['division'].lower()}", axis=1
    )
    metadata_day_div_json["name"] = metadata_day_div_json.apply(
        lambda x: f"D{x['division']} - Journée {x['day']}", axis=1
    )

    metadata_day_div_json = metadata_day_div_json.rename(
        columns={"datetime_start": "date"}
    )

    metadata_day_div_json["date"] = metadata_day_div_json["date"].apply(str)

    return metadata_day_div_json.set_index("calendar_id").to_dict("index")
