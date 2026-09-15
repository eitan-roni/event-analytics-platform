from fastapi import FastAPI

from app.api.events import router as events_router

app = FastAPI(title="Event Analytics Platform")
app.include_router(events_router)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
