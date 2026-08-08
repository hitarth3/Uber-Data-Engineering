"""
FastAPI Ride Simulation & Event Streaming Application.
"""

import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from connection import send_to_event_hub
from data import generate_uber_ride_confirmation

app = FastAPI(
    title="Uber Real-Time Data Pipeline Producer",
    description="FastAPI service simulating real-time Uber bookings and streaming events to Azure Event Hubs.",
    version="1.0.0"
)

templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    """Renders the main booking simulation dashboard."""
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/book", response_class=HTMLResponse)
def book_ride(request: Request):
    """Generates a synthetic ride confirmation payload and dispatches it to Event Hubs."""
    ride = generate_uber_ride_confirmation()
    send_to_event_hub(ride)
    return templates.TemplateResponse("confirmation.html", {"request": request, "ride": ride})


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "uber-event-producer"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    uvicorn.run("api:app", host=host, port=port, reload=True)
