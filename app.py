from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Any
from datetime import datetime

app = FastAPI(title="Insight Dashboard")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

transcript: list[dict[str, Any]] = []
connected_clients: list[WebSocket] = []

session_state = {
    "session_title": "Live Session",
    "clinician": "Unassigned",
    "client": "Active Client",
    "started_at": None,
    "notes": "",
    "tags": [],
}


def normalize_segments(body: Any) -> list[dict[str, Any]]:
    if isinstance(body, dict) and isinstance(body.get("segments"), list):
        source = body["segments"]
    elif isinstance(body, list):
        source = body
    elif isinstance(body, dict) and body.get("text"):
        source = [body]
    else:
        return []

    normalized = []
    for item in source:
        if isinstance(item, str):
            normalized.append(
                {
                    "id": None,
                    "speaker": None,
                    "text": item,
                    "timestamp": None,
                    "received_at": datetime.utcnow().isoformat(),
                }
            )
        elif isinstance(item, dict):
            normalized.append(
                {
                    "id": item.get("id"),
                    "speaker": item.get("speaker"),
                    "text": item.get("text", ""),
                    "timestamp": item.get("timestamp"),
                    "start": item.get("start"),
                    "end": item.get("end"),
                    "received_at": datetime.utcnow().isoformat(),
                }
            )
    return normalized


def build_payload():
    return {
        "type": "dashboard_update",
        "segments": transcript,
        "session": session_state,
        "count": len(transcript),
    }


async def broadcast_state():
    payload = build_payload()
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
    return templates.TemplateResponse("insight-dashboard.html", {"request": request})


@app.get("/health")
async def health():
    return {
        "ok": True,
        "segments": len(transcript),
        "clients": len(connected_clients),
        "session": session_state,
    }


@app.get("/transcript")
async def get_transcript():
    return build_payload()


@app.delete("/transcript")
async def clear_transcript():
    transcript.clear()
    session_state["started_at"] = None
    await broadcast_state()
    return {"status": "cleared"}


@app.post("/notes")
async def update_notes(request: Request):
    body = await request.json()
    session_state["notes"] = body.get("notes", "")
    session_state["tags"] = body.get("tags", [])
    await broad

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

    if not session_state["started_at"]:
        session_state["started_at"] = datetime.utcnow().isoformat()

    transcript.extend(new_segments)
    print(f"Added {len(new_segments)} segments")

    await broadcast_state()

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
        await websocket.send_json(build_payload())
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
    except Exception:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
