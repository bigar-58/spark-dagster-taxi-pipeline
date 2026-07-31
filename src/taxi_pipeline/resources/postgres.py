import dagster as dg

from taxi_pipeline.config import PostgresSettings

@dg.resource 
def postgres_settings_resource() -> PostgresSettings:
    """Load postgres settings from env"""
    
    return PostgresSettings.from_env()
