import asyncio, json, random, string, os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx

app = FastAPI()

# ── Room state ──
rooms: dict = {}  # code -> room dict
room_connections: dict = {}  # code -> {pid: websocket}

THEMES = {
    "computer": "Computer-Teile",
    "autos": "Autos",
    "tiere": "Tiere",
    "laender": "Länder",
    "sport": "Sport",
    "essen": "Essen",
    "musik": "Musik",
    "berufe": "Berufe",
}

FALLBACK_WORDS = {
    "computer": ["prozessor","speicher","grafikkarte","mainboard","festplatte","tastatur","maus","monitor","netzteil","cpu","ram","ssd","usb","kabel","gehäuse","lüfter","chip","display","drucker","scanner","platine","router","server","netzwerk","pixel"],
    "autos": ["motor","getriebe","bremse","reifen","lenkrad","kupplung","auspuff","kühler","batterie","scheinwerfer","kotflügel","karosserie","spiegel","sitz","fenster","stoßstange","tank","benzin","felge","achse"],
    "tiere": ["löwe","tiger","elefant","giraffe","pinguin","delfin","adler","wolf","bär","fuchs","hase","reh","kuh","schwein","katze","hund","pferd","vogel","schlange","frosch","krokodil","affe","zebra"],
    "laender": ["deutschland","frankreich","spanien","italien","österreich","schweiz","portugal","griechenland","türkei","japan","china","brasilien","mexiko","kanada","australien","indien","norwegen","schweden","dänemark"],
    "sport": ["fussball","tennis","schwimmen","radfahren","laufen","turnen","boxen","basketball","volleyball","handball","golf","reiten","segeln","klettern","skifahren","eishockey","rugby","tischtennis"],
    "essen": ["pizza","pasta","suppe","salat","brot","butter","käse","schinken","tomaten","kartoffeln","reis","fleisch","fisch","ei","milch","joghurt","kuchen","keks","schokolade","eis","apfel","banane"],
    "musik": ["gitarre","klavier","schlagzeug","geige","flöte","trompete","cello","saxofon","orgel","harfe","bass","keyboard","mikrofon","noten","melodie","rhythmus","chor","konzert"],
    "berufe": ["arzt","lehrer","ingenieur","koch","bäcker","maler","fahrer","pilot","richter","anwalt","architekt","apotheker","polizist","bauer","kaufmann","kellner","friseur","elektriker"],
}

def gen_code():
    return ''.join(random.choices(string.ascii_uppercase, k=6))

async def fetch_words(theme: str, count: int) -> list:
    label = THEMES.get(theme, theme)
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return build_fallback(theme, count)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 800,
                    "messages": [{"role": "user", "content":
                        f"Gib mir {count} deutsche Wörter zum Thema '{label}'. "
                        "Nur einfache Nomen, ohne Artikel, ohne Nummerierung, "
                        "ein Wort pro Zeile, kein Satzzeichen, alles Kleinbuchstaben, maximal 12 Buchstaben."
                    }]
                }
            )
            data = r.json()
            text = data["content"][0]["text"]
            words = [w.strip().lower() for w in text.splitlines() if w.strip()]
            words = [w for w in words if w.isalpha() and 2 <= len(w) <= 12]
            if len(words) >= 15:
                return words
    except Exception as e:
        print(f"API error: {e}")
    return build_fallback(theme, count)

def build_fallback(theme: str, count: int) -> list:
    pool = FALLBACK_WORDS.get(theme, FALLBACK_WORDS["computer"])
    out = []
    while len(out) < count:
        out.extend(pool)
    random.shuffle(out)
    return out[:count]

async def broadcast(code: str, msg: dict):
    conns = room_connections.get(code, {})
    dead = []
    for pid, ws in conns.items():
        try:
            await ws.send_text(json.dumps(msg))
        except:
            dead.append(pid)
    for pid in dead:
        conns.pop(pid, None)

@app.get("/")
async def index():
    return FileResponse("templates/index.html")

@app.websocket("/ws/{code}/{pid}")
async def websocket_endpoint(ws: WebSocket, code: str, pid: str):
    await ws.accept()
    room_connections.setdefault(code, {})[pid] = ws
    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            await handle_message(code, pid, msg)
    except WebSocketDisconnect:
        room_connections.get(code, {}).pop(pid, None)
        room = rooms.get(code)
        if room and pid in room.get("players", {}):
            room["players"].pop(pid, None)
            if not room["players"]:
                rooms.pop(code, None)
            else:
                await broadcast(code, {"type": "room_update", "room": safe_room(code)})

async def handle_message(code: str, pid: str, msg: dict):
    t = msg.get("type")

    if t == "create_room":
        room = {
            "code": code,
            "host": pid,
            "theme": msg["theme"],
            "goal": msg["goal"],
            "status": "waiting",
            "words": [],
            "players": {
                pid: {"name": msg["name"], "score": 0, "wrong": 0, "finished": False, "finishTime": None}
            }
        }
        rooms[code] = room
        await broadcast(code, {"type": "room_update", "room": safe_room(code)})

    elif t == "join_room":
        room = rooms.get(code)
        if not room:
            await room_connections[code][pid].send_text(json.dumps({"type": "error", "msg": "Raum nicht gefunden"}))
            return
        if room["status"] != "waiting":
            await room_connections[code][pid].send_text(json.dumps({"type": "error", "msg": "Spiel läuft bereits"}))
            return
        room["players"][pid] = {"name": msg["name"], "score": 0, "wrong": 0, "finished": False, "finishTime": None}
        await broadcast(code, {"type": "room_update", "room": safe_room(code)})

    elif t == "start_game":
        room = rooms.get(code)
        if not room or room["host"] != pid:
            return
        await broadcast(code, {"type": "loading"})
        words = await fetch_words(room["theme"], room["goal"] + 40)
        room["words"] = words
        room["status"] = "playing"
        room["startAt"] = asyncio.get_event_loop().time() * 1000 + 3500
        await broadcast(code, {"type": "game_start", "room": safe_room(code)})

    elif t == "update_score":
        room = rooms.get(code)
        if not room:
            return
        p = room["players"].get(pid)
        if not p:
            return
        p["score"] = msg["score"]
        p["wrong"] = msg["wrong"]
        if msg.get("finished") and not p["finished"]:
            p["finished"] = True
            p["finishTime"] = msg.get("finishTime")
            room["status"] = "done"
        await broadcast(code, {"type": "score_update", "players": room["players"]})
        if room["status"] == "done":
            await broadcast(code, {"type": "game_over", "room": safe_room(code)})

def safe_room(code: str) -> dict:
    room = rooms.get(code, {})
    return {k: v for k, v in room.items()}

app.mount("/static", StaticFiles(directory="static"), name="static")
