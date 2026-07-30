from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PostgresSettings:
    """Connection settings for the local Postgres warehouse."""

    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)
    
    
    @classmethod
    def from_env(cls) -> PostgresSettings:
        required_variables = (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD"
        )

        missing_variables = [variable for variable in required_variables if not os.getenv(variable)]

        if missing_variables:
            raise RuntimeError(f"Missing required Postgres environment variables: {missing_variables}")

        try:
            port = int(os.environ["POSTGRES_PORT"])
        except ValueError as exc:
            raise RuntimeError("POSTGRES_PORT must be an integer") from exc

        return cls(
            host=os.environ["POSTGRES_HOST"],
            port=port,
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"]
        )

    def connection_kwargs(self) -> dict[str, str | int]:
        """Return keyword arguments accepted by psycopg.connect()."""

        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": 5
        }