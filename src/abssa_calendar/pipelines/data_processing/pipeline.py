"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline

from .join import get_clubs_teams, get_events, get_metadata
from .transform import cast_fixtures, process_clubs, process_fields, process_fixtures


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=cast_fixtures,
                inputs="fixtures_raw",
                outputs="fixtures_intermediate",
                name="cast_fixtures",
            ),
            node(
                func=process_fixtures,
                inputs="fixtures_intermediate",
                outputs="fixtures_primary",
                name="process_fixtures",
            ),
            node(
                func=process_fields,
                inputs="fields_raw",
                outputs="fields_primary",
                name="process_fields",
            ),
            node(
                func=process_clubs,
                inputs="clubs_raw",
                outputs=["clubs_primary", "teams_primary"],
                name="process_clubs",
            ),
            node(
                func=get_clubs_teams,
                inputs=["clubs_primary", "teams_primary"],
                outputs="clubs_teams_primary",
                name="get_clubs_teams",
            ),
            node(
                func=get_events,
                inputs=["fixtures_primary", "clubs_teams_primary", "fields_primary"],
                outputs="events_feature",
                name="get_events",
            ),
            node(
                func=get_metadata,
                inputs="clubs_teams_primary",
                outputs="metadata_feature",
                name="get_metadata",
            ),
        ]
    )
