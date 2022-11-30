"""
This is a boilerplate pipeline 'make_ical'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import get_calendars, get_metadata_json


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=get_calendars,
                inputs=["events_feature", "metadata_feature"],
                outputs="ics_mo",
                name="get_calendars",
            ),
            node(
                func=get_metadata_json,
                inputs="metadata_feature",
                outputs="metadata_mo",
                name="get_metadata_json",
            ),
        ]
    )
