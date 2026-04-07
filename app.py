from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Any
import json

app = FastAPI(title="Insight")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

transcript: list[dict[str, Any]] = []
connected_clients: list[WebSocket] = []


def normalize_segments(body: Any) -> list[dict[str, Any]]:
    if isinstance(body, dict) and isinstance(body.get("segments"), list):
        return body["segments"]

    if isinstance(body, list):
        normalized = []
        for item in body:
            if isinstance(item, str):
                normalized.append({"text": item, "speaker": None})
            elif isinstance(item, dict):
                normalized.append(
                    {
                        "id": item.get("id"),
                        "text": item.get("text", ""),
                        "speaker": item.get("speaker"),
                        "timestamp": item.get("timestamp"),
                        "start": item.get("start"),
                        "end": item.get("end"),
                    }
                )
        return normalized

    if isinstance(body, dict) and body.get("text"):
        return [
            {
                "id": body.get("id"),
                "text": body.get("text", ""),
                "speaker": body.get("speaker"),
                "timestamp": body.get("timestamp"),
                "start": body.get("start"),
                "end": body.get("end"),
            }
        ]

    return []


async def broadcast_transcript():
    payload = {"type": "transcript_update", "segments": transcript}
    dead_clients = []

    for client in connected_clients:
        try:
            await client.send_json(payload)
        except Exception:
            dead_clients.append(client)

    for client in dead_clients:
        if client in connected_clients:
            connected_clients.remove(client)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("insight-live.html", {"request": request})


@app.get("/health")
async def health():
    return {
        "ok": True,
        "segments": len(transcript),
        "clients": len(connected_clients),
    }


@app.get("/transcript")
async def get_transcript():
    return {"segments": transcript, "count": len(transcript)}


@app.delete("/transcript")
async def clear_transcript():
    transcript.clear()
    await broadcast_transcript()
    return {"status": "cleared"}


@app.post("/")
@app.post("/webhook")
@app.post("/ingest")
async def ingest_transcript(request: Request, uid: str | None = None):
    raw = await request.body()
    print("=== WEBHOOK HIT ===")
    print("uid:", uid)
    print("Headers:", dict(request.headers))
    print("Raw body:", raw.decode("utf-8", errors="ignore"))

    try:
        body = await request.json()
        print("Parsed JSON type:", type(body).__name__)
        print("Parsed JSON:", body)
    except Exception as e:
        print("JSON parse failed:", e)
        return {"ok": False, "error": "invalid json"}

    new_segments = normalize_segments(body)

    if not new_segments:
        print("Unexpected payload shape")
        return {"ok": False, "error": "unexpected payload shape"}

    transcript.extend(new_segments)
    print(f"Added {len(new_segments)} segments")

    await broadcast_transcript()

    return {
        "ok": True,
        "uid": uid,
        "added": len(new_segments),
        "total": len(transcript),
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)

    try:
        await websocket.send_json(
            {"type": "transcript_update", "segments": transcript}
        )

        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
    except Exception:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
