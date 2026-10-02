from fastapi import FastAPI

from . import naive

app = FastAPI(title="ticket-drop")
app.include_router(naive.router)
