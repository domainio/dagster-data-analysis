from pydantic import BaseModel, Field
from dagster import resource

class DatabaseConfig(BaseModel):
    host: str
    port: int = Field(5432, ge=1, le=65535)
    username: str
    password: str

@resource(config_schema=DatabaseConfig)
def database_resource(init_context):
    config = DatabaseConfig(**init_context.resource_config)
    # Use config to establish database connection
    # ... database connection logic ...
    return database_connection
