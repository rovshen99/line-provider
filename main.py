import json
from datetime import datetime, timedelta
from enum import Enum
from typing import Union, Dict, List

import aio_pika
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

app = FastAPI()


class EventStatus(str, Enum):
    PENDING = "pending"
    WIN = "win"
    LOSE = "lose"


class Event(BaseModel):
    id: str
    coefficient: float = Field(
        gt=0, description="А strictly positive number with two decimal places"
    )
    deadline: datetime
    status: EventStatus

    @validator("coefficient")
    def validate_coefficient(cls, v):
        if round(v, 2) != v:
            raise ValueError("Coefficient must have two decimal places")
        return v


events: Dict[str, Event] = {
    "1": Event(
        id="1",
        coefficient=1.2,
        deadline=datetime.now() + timedelta(minutes=10),
        status=EventStatus.PENDING,
    ),
    "2": Event(
        id="2",
        coefficient=1.15,
        deadline=datetime.now() + timedelta(minutes=1),
        status=EventStatus.PENDING,
    ),
    "3": Event(
        id="3",
        coefficient=1.67,
        deadline=datetime.now() + timedelta(minutes=1.5),
        status=EventStatus.PENDING,
    ),
}


async def send_message(event_id: str, status: str) -> None:
    try:
        connection = await aio_pika.connect("amqp://guest:guest@localhost:5672/")
        channel = await connection.channel()
        queue = await channel.declare_queue("events", durable=True)
        message_body = json.dumps({"event_id": event_id, "status": status})
        message = aio_pika.Message(body=message_body.encode())
        await channel.default_exchange.publish(
            message,
            routing_key=queue.name,
        )
        print(f"Sent message: {message_body}")
        await connection.close()
    except Exception as e:
        print(f"Failed to send message: {e}")


@app.post("/event", response_model=Event)
async def create_event(event: Event) -> Union[Event, JSONResponse]:
    if event.id in events:
        return JSONResponse(
            content={"detail": "Event with this ID already exists"}, status_code=400
        )
    events[event.id] = event
    try:
        await send_message(event.id, event.status.value)
    except Exception as e:
        return JSONResponse(content={"detail": str(e)}, status_code=500)
    return event


@app.put("/event/{event_id}", response_model=Event)
async def update_event(
    event_id: str, status: EventStatus
) -> Union[Event, JSONResponse]:
    if event_id in events:
        events[event_id].status = status
        await send_message(event_id, status.value)
        return events[event_id]
    return JSONResponse(content={"detail": "Event not found"}, status_code=404)


@app.get("/event/{event_id}", response_model=Event)
async def get_event(event_id: str) -> Union[Event, JSONResponse]:
    if event_id in events:
        return events[event_id]
    return JSONResponse(content={"detail": "Event not found"}, status_code=404)


@app.get("/events", response_model=List[Event])
async def get_events() -> List[Event]:
    return [event for event in events.values() if event.deadline > datetime.now()]
