"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""
import datetime
import re

import numpy as np
import pandas as pd
from geopy.geocoders import GoogleV3
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from spacy.lang.fr.stop_words import STOP_WORDS as fr_stop
from thefuzz import fuzz
from tqdm import tqdm

project_path = "."
conf_path = project_path + "/" + settings.CONF_SOURCE
conf_loader = ConfigLoader(conf_source=conf_path, env="local")
credentials = conf_loader.get("credentials*", "credentials*/**")
# from geopy.geocoders import Nominatim


PLACE_MAX_WORD_COUNT = 20


def capitalize(txt):
    words = txt.lower().split(" ")
    words = [w.title() if w not in fr_stop else w for w in words]
    return " ".join(words)


def cast_fixtures(fixtures_df):
    fixtures_df["date"] = pd.to_datetime(fixtures_df["date"], dayfirst=True)
    return fixtures_df


def process_fixtures(fixtures_df):
    s = fixtures_df["home_club_id"] == "999"
    s |= fixtures_df["away_club_id"] == "999"
    fixtures_df = fixtures_df.loc[~s]

    fixtures_df["played"] = fixtures_df["time"].apply(lambda x: ":" not in x)
    fixtures_df["result"] = np.nan
    fixtures_df.loc[fixtures_df["played"], "result"] = fixtures_df.loc[
        fixtures_df["played"], "time"
    ]
    fixtures_df.loc[fixtures_df["played"], "time"] = np.nan

    # infer time of played games by using most frequent
    scope = ~fixtures_df["played"]
    game_time = (
        fixtures_df.loc[scope]
        .groupby(["home_club_id", "division"], as_index=False)["time"]
        .apply(lambda x: x.mode())
    )
    game_time = game_time.rename(columns={0: "time_most_frequent"})
    fixtures_df = fixtures_df.merge(game_time, on=("home_club_id", "division"))
    fixtures_df["time"] = (
        fixtures_df[["time", "time_most_frequent"]].bfill(axis=1).iloc[:, 0]
    )

    fixtures_df["datetime_start"] = np.nan
    fixtures_df["datetime_start"] = pd.to_datetime(
        fixtures_df["date"] + " " + fixtures_df["time"] + " CET"
    )

    fixtures_df["datetime_end"] = fixtures_df["datetime_start"] + datetime.timedelta(
        minutes=35 * 2
    )

    return fixtures_df


def process_fields(fields_df):

    fields_df = fields_df.rename(
        columns={
            "Code": "code",
            "Rue": "street",
            "Localité": "municipality",
            "Synthétique": "artificial_grass",
            "Stabilisé": "stabilized_field",
            "Situation": "place",
            "Acces voiture": "car_access",
        }
    )

    fields_df["place"] = fields_df["place"].fillna("")
    fields_df["place"] = fields_df["place"].apply(capitalize)

    place_to_fix = fields_df["place"].str.split().apply(len) > PLACE_MAX_WORD_COUNT
    empty_car_access = fields_df["car_access"].isna()
    fill_car_access_by_place = (place_to_fix) & (empty_car_access)
    fields_df.loc[fill_car_access_by_place, "car_access"] = fields_df.loc[
        fill_car_access_by_place, "place"
    ]
    fields_df.loc[place_to_fix, "place"] = ""

    place_street_fuzz = fields_df.apply(
        lambda x: fuzz.partial_ratio(x["place"], x["street"]), axis=1
    )
    place_is_street = place_street_fuzz >= 95
    fields_df.loc[place_is_street, "place"] = ""

    fields_df["street"] = (
        fields_df["street"].str.replace(", ", " ").str.replace(",", " ")
    )

    empty_place = fields_df["place"].str.len() != 0
    field_address = pd.concat(
        (
            fields_df.loc[empty_place, ["place", "street", "municipality"]].agg(
                ", ".join, axis=1
            ),
            fields_df.loc[~empty_place, ["street", "municipality"]].agg(
                ", ".join, axis=1
            ),
        )
    )
    field_address += ", Belgique"
    fields_df["address"] = field_address

    fields_df["code"] = fields_df["code"].str.replace(r"\W+\)", ")", regex=True)

    geolocator = GoogleV3(api_key=credentials["google_map_api_key"])
    # geolocator = Nominatim(user_agent="abssa-calendar")

    tqdm.pandas(desc="Geocoding field address...")

    field_address_geocode = field_address.progress_map(geolocator.geocode)
    geocode_success = field_address_geocode.notna()

    fields_df["latitude"] = np.nan
    fields_df["longitude"] = np.nan

    fields_df.loc[geocode_success, "latitude"] = field_address_geocode.loc[
        geocode_success
    ].apply(lambda x: x.latitude)
    fields_df.loc[geocode_success, "longitude"] = field_address_geocode.loc[
        geocode_success
    ].apply(lambda x: x.longitude)

    return fields_df


def get_extra_club_info(info_terrain):
    club_info = pd.Series(dtype="object")
    info_terrain = info_terrain.splitlines()

    match = re.search(r"\. +([^\t]+)", info_terrain[0])
    club_info["club_name"] = match.group(1)

    match = re.search(r"\d+$", info_terrain[0])
    club_info["club_creation_year"] = match.group(0)

    return club_info


def get_teams_info(info_terrain, club_id, team_colors):
    info_terrain = info_terrain.splitlines()
    teams_df = pd.DataFrame()

    i = 1
    team_line = info_terrain[i]
    while team_line != "":
        team_info = pd.Series(dtype="object")
        team_info["club_id"] = club_id
        team_info["team_id"] = i
        team_line = ":".join(team_line.split(":")[1:]).strip()
        team_line = re.sub(r"\s+", " ", team_line)

        team_info["game_time"] = team_line.split(" ")[0]
        team_info["division"] = team_line.split(" ")[1]
        team_info["artificial_grass"] = len(team_line.split(" ")) > 2

        teams_df = pd.concat([teams_df, pd.DataFrame(team_info).T], ignore_index=True)

        i += 1
        team_line = info_terrain[i]

    if i + 1 == len(info_terrain) - 1:
        teams_df["field_id"] = info_terrain[-1].split("-")[0].replace(" ", "")
    else:
        teams_df["field_id"] = np.nan
        for j in range(i + 1, len(info_terrain)):
            field_line = info_terrain[j]
            field_id = field_line.split("-")[0]
            field_id = field_id.replace(" ", "")
            team_desc = re.search(r"quipe.+", field_line).group(0)
            team_ids = list(map(int, re.findall(r"\d", team_desc)))
            scope = teams_df["team_id"].isin(team_ids)
            teams_df.loc[scope, "field_id"] = field_id

    if len(team_colors.splitlines()) == 1:
        teams_df["color"] = team_colors
    else:
        teams_df["color"] = np.nan
        for team_color in team_colors.splitlines():
            team_desc, color_desc = team_color.split(":")
            team_desc = re.search(r"quipe.+", team_desc).group(0)
            team_ids = list(map(int, re.findall(r"\d", team_desc)))
            color_desc = color_desc.strip()
            scope = teams_df["team_id"].isin(team_ids)
            teams_df.loc[scope, "color"] = color_desc

    return teams_df


def process_clubs(clubs_df):

    clubs_df = clubs_df.rename(
        columns={
            "Secretaire": "secretary",
            "Adresse": "street",
            "Localite": "municipality",
            "Tel": "phone_number",
            "Fax": "fax",
            "Tel Bureau": "office_phone_number",
            "E-mail": "email",
            "Gsm": "mobile_phone_number",
            "Couleurs": "color",
        }
    )

    extra_club_info = clubs_df["info_terrain"].apply(get_extra_club_info)
    clubs_df = clubs_df.join(extra_club_info)

    teams_df = clubs_df.apply(
        lambda x: get_teams_info(x["info_terrain"], x["club_id"], x["color"]), axis=1
    )

    teams_df = pd.concat(teams_df.tolist())

    clubs_df = clubs_df.drop("info_terrain", axis=1)

    return clubs_df, teams_df
