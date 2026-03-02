from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.lifecycle import lifespan
from app.routes import admin, chat, pipeline, search, system


app = FastAPI(title="Video Semantic Search API", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(system.router)
app.include_router(admin.router)
app.include_router(pipeline.router)
app.include_router(search.router)
app.include_router(chat.router)
