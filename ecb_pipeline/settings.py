from os.path import dirname, join

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import os

# import ecb_pipeline

# ECB_LOCAL_DIR = dirname(dirname(ecb_pipeline.__file__))
# Define the project directory

ECB_LOCAL_DIR = os.getenv("ECB_LOCAL_DIR")


class DBCredentials(BaseSettings):
    """Use env variables prefixed with SUNNDAY_DB_"""

    host: str
    port: int = 5432
    user: str
    password: str
    dbname: str
    model_config = SettingsConfigDict(env_prefix="sunday_db_")


class Settings(BaseSettings):

    local_dir: str = Field(
        default=join(ECB_LOCAL_DIR, "ecb_data"),
        description="For any other value set env variable 'ECB_LOCAL_DIR'",
    )
    telemetry_dsn: str = "http://project2_secret_token@uptrace:14317/2"

    model_config = SettingsConfigDict(env_prefix="ecb_pipeline_")

    @property
    def raw_dir(self) -> str:
        """Store inside all the raw jsons"""
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        return join(self.local_dir, "prepared")
