"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

from slugify import slugify


def get_clubs_teams(clubs_df, teams_df):
    teams_clubs_df = teams_df.merge(
        clubs_df[
            ["club_id", "club_name", "secretary", "phone_number", "email", "club_url"]
        ],
        on="club_id",
    )
    teams_clubs_df["club_team_id"] = (
        teams_clubs_df["club_id"].apply(str)
        + "_"
        + teams_clubs_df["team_id"].apply(str)
    )
    teams_clubs_df["club_team_name"] = teams_clubs_df["club_name"]
    teams_clubs_df["club_team_name"] += teams_clubs_df.apply(
        lambda x: "" if x["team_id"] == 1 else f" (Eq. {x['team_id']})",
        axis=1,
    )

    return teams_clubs_df


def get_events(fixtures_df, team_club_df, fields_df):
    events_df = fixtures_df
    events_df = events_df[
        [
            "division",
            "day",
            "datetime_start",
            "datetime_end",
            "home_club_id",
            "away_club_id",
        ]
    ]
    events_df = events_df.merge(
        team_club_df,
        left_on=["home_club_id", "division"],
        right_on=["club_id", "division"],
    )
    events_df = events_df.merge(
        team_club_df,
        left_on=["away_club_id", "division"],
        right_on=["club_id", "division"],
        suffixes=("_home", "_away"),
    )
    events_df = events_df.merge(fields_df, left_on="field_id_home", right_on="field_id")

    summary = "ABSSA D{division} J{day}: {club_team_name_home} vs {club_team_name_away}"
    description = (
        "Terrain synthétique: {artificial_grass_home}\n\nCouleur"
        " principale équipe domicile: {color_home}\nCouleur principale"
        " équipe exterieure: {color_away}\n\nContact équipe domicile:"
        " {secretary_home} ({phone_number_home} - {email_home})\n\nAccès voiture :"
        " {car_access}\n\nInfo équipe domicile: {home_team_link}\nInfo équipe exterieure: {away_team_link}\nInfo terrain: {field_link}"
    )

    events_df["ical_summary"] = events_df.apply(
        lambda x: summary.format(
            division=x["division"],
            day=x["day"],
            club_team_name_home=x["club_team_name_home"],
            club_team_name_away=x["club_team_name_away"],
        ),
        axis=1,
    )

    events_df["ical_desc"] = events_df.apply(
        lambda x: description.format(
            artificial_grass_home="oui" if x["artificial_grass_home"] else "non",
            color_home=x["color_home"],
            color_away=x["color_away"],
            secretary_home=x["secretary_home"],
            phone_number_home=x["phone_number_home"],
            car_access=x["car_access"],
            home_team_link=x["club_url_home"],
            away_team_link=x["club_url_away"],
            field_link=x["field_url"],
            email_home=x["email_home"],
        ),
        axis=1,
    )

    events_df = events_df.sort_values("day")

    return events_df


def get_metadata(clubs_teams_df):
    c = ["club_name", "club_id", "team_id", "club_team_id", "club_team_name"]
    get_metadata_df = clubs_teams_df[c]
    get_metadata_df["calendar_id"] = (
        get_metadata_df["club_team_id"]
        + "_"
        + get_metadata_df["club_name"].apply(slugify, separator="_")
    )
    calendar_name = "ABSSA: {club_name} (eq. {team_id})"
    calendar_desc = "Les matches du club {club_name} (equipe {team_id}) en ABSSA."
    calendar_search = "{club_id} - {club_name} (eq. {team_id})"

    get_metadata_df["calname"] = get_metadata_df.apply(
        lambda x: calendar_name.format(
            club_name=x["club_name"],
            team_id=x["team_id"],
        ),
        axis=1,
    )

    get_metadata_df["caldesc"] = get_metadata_df.apply(
        lambda x: calendar_desc.format(
            club_name=x["club_name"],
            team_id=x["team_id"],
        ),
        axis=1,
    )

    get_metadata_df["search_name"] = get_metadata_df.apply(
        lambda x: calendar_search.format(
            club_name=x["club_name"],
            team_id=x["team_id"],
            club_id=str(x["club_id"]).zfill(3),
        ),
        axis=1,
    )

    return get_metadata_df
