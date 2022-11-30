"""
This is a boilerplate pipeline 'make_ical'
generated using Kedro 0.18.3
"""
import datetime
import uuid

from icalendar import Calendar, Event, vCalAddress, vGeo, vText


def generate_ics(events_df, name, desc, calendar_id):
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
        ev["dtstart"] = event["datetime_start"].strftime("%Y%m%dT%H%M%SZ")
        ev["dtend"] = event["datetime_end"].strftime("%Y%m%dT%H%M%SZ")
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
        name = f"Journée {day}"
        desc = f"Les matches de la journée {day} en ABSSA."
        calendar_id = f"abbsa_j_{day}"

        scope = events_df["day"] == day

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
