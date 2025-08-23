from contextlib import asynccontextmanager

from fastapi import FastAPI

from packages.redshift.client import initialize_redshift_pool, redshift_connection_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager for FastAPI.

    This function is used to manage the lifespan of the application,
    allowing for setup and teardown operations.
    """

    initialize_redshift_pool()

    yield

    while not redshift_connection_pool.empty():
        try:
            conn = redshift_connection_pool.get_nowait()
            conn.close()  # Close the connection
        except Exception as e:
            print(f"Error closing Redshift connection: {e}")
