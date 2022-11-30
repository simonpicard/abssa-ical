"""
This is a boilerplate pipeline 'data_scrapping'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import scrap_fields, scrap_clubs, scrap_fixtures


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=scrap_fields,
                inputs=None,
                outputs="fields_raw",
                name="scrap_fields",
            ),
            node(
                func=scrap_clubs,
                inputs=None,
                outputs="clubs_raw",
                name="scrap_clubs",
            ),
            node(
                func=scrap_fixtures,
                inputs=None,
                outputs="fixtures_raw",
                name="scrap_fixtures",
            ),
        ]
    )
