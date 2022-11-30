"""
This is a boilerplate pipeline 'data_scrapping'
generated using Kedro 0.18.3
"""

import json
import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def scrap_fields():
    logging.info("Scrapping fields...")
    # fetch list of fields url
    url = "https://www.abssa.org/terrains"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    terrain_list = soup.find("ul", {"class": "terrain-list"})

    bool_var = ["Synthétique", "Stabilisé"]
    fields_df = pd.DataFrame()
    # scrap each field
    for field_a in tqdm(terrain_list.find_all("a")):
        field_url = field_a["href"]
        res = requests.get(field_url)
        soup = BeautifulSoup(res.text, "html.parser")
        field_ul = soup.find("ul", {"class": "abssa_list"})
        field_lis = field_ul.find_all("li")

        field_info = pd.Series(dtype="object")
        field_info["field_id"] = field_url.split("/")[-1]
        field_info["field_url"] = field_url

        for li in field_lis:
            if li.span:
                name = li.span.text
                if name in bool_var:
                    val = "glyphicon-ok" in li.find("i")["class"]
                else:
                    val = li.text
                    val = val[len(name) + 1 :]
                    val = val.strip()
                field_info[name] = val

        fields_df = pd.concat(
            [fields_df, pd.DataFrame(field_info).T], ignore_index=True
        )

    return fields_df


def scrap_clubs():
    logging.info("Scrapping clubs and teams...")
    # fetch teams url

    url = "https://www.abssa.org/teams"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    team_list = soup.find("ul", {"class": "team-list"})

    # scrap clubs and teams

    clubs_df = pd.DataFrame()

    for club_a in tqdm(team_list.find_all("a")):
        club_url = club_a["href"]

        club_info = pd.Series(dtype="object")
        club_info["club_slug"] = club_url.split("/")[-1]
        club_info["club_id"] = club_url.split("/")[-2]
        club_info["club_url"] = club_url

        res = requests.get(club_url)
        soup = BeautifulSoup(res.text, "html.parser")
        club_uls = soup.find_all("ul", {"class": "abssa_list"})
        for i in range(2):
            club_lis = club_uls[i].find_all("li")
            for li in club_lis:
                if li.span:
                    name = li.span.text
                    val = li.text
                    val = val[len(name) + 1 :]
                    val = val.strip()
                    club_info[name] = val

        info_terrain = club_uls[2].find_all("li")[1].text
        club_info["info_terrain"] = info_terrain

        clubs_df = pd.concat([clubs_df, pd.DataFrame(club_info).T], ignore_index=True)

    return clubs_df


def scrap_fixtures():
    logging.info("Scrapping fixtures...")
    # get division urls
    url = "http://www.abssa.org/championnat"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    ul = soup.find_all("ul", {"class": "dropdown-menu"})

    # get days ids
    lis = ul[1].find_all("li")
    game_days = []
    game_days_value = []
    for i in range(len(lis)):
        li = lis[i]
        game_days.append(li.text)
        game_days_value.append(li.a["data-value"])

    # scrap fixtures
    fixtures_df = pd.DataFrame()
    for i in tqdm(range(len(game_days))):
        day_df = pd.DataFrame()
        url = f"http://www.abssa.org/amatches/{game_days_value[i]}"
        res = requests.get(url)
        res = json.loads(res.text)

        for division in res:
            division_df = pd.DataFrame()
            for fixture in division["m"]:
                fixture_info = pd.Series(dtype="object")
                fixture_info["home_club_id"] = fixture["mi"]
                fixture_info["away_club_id"] = fixture["mo"]
                fixture_info["time"] = fixture["ds"]
                division_df = pd.concat(
                    [division_df, pd.DataFrame(fixture_info).T], ignore_index=True
                )
            division_df["division"] = str(division["d"])
            day_df = pd.concat([day_df, division_df], ignore_index=True)
        day_df["date"] = game_days[i]
        day_df["day"] = i + 1
        fixtures_df = pd.concat([fixtures_df, day_df], ignore_index=True)

    return fixtures_df
