import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import booking, expiry, naive
from .db import init_db


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    sweeper = asyncio.create_task(expiry.run())
    yield
    sweeper.cancel()


app = FastAPI(title="ticket-drop", lifespan=lifespan)
app.include_router(naive.router)
app.include_router(booking.router)
