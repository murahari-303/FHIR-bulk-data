import uuid
import os
from fastapi import FastAPI, Request, Response, Header, HTTPException, status
from fastapi.responses import JSONResponse, StreamingResponse
from lxml import etree
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI(title="Simplified FHIR & C-CDA Engine")

# File storage directory for downloads
EXPORT_DIR = "./export_data"
os.makedirs(EXPORT_DIR, exist_ok=True)

# In-memory job tracker (Simulating DB/Redis for simplicity)
JOBS_DB = {}

# ==========================================
# PART 1: FHIR BULK DATA EXPORT (ASYNC FLOW)
# ==========================================

@app.get("/fhir/v1/Patient/$export", status_code=status.HTTP_202_ACCEPTED)
def kick_off_export(response: Response, prefer: str = Header(None)):
    if prefer != "respond-async":
        raise HTTPException(status_code=400, detail="Header 'Prefer: respond-async' is required.")
    
    job_id = str(uuid.uuid4())
    
    # Create mock NDJSON output file
    file_name = f"{job_id}_Patient.ndjson"
    file_path = os.path.join(EXPORT_DIR, file_name)
    
    with open(file_path, "w") as f:
        f.write('{"resourceType":"Patient","id":"p1","gender":"female"}\n')
        f.write('{"resourceType":"Patient","id":"p2","gender":"male"}\n')

    # Save job metadata
    JOBS_DB[job_id] = {
        "status": "completed",
        "file_name": file_name
    }

    # Set status polling location header
    response.headers["Content-Location"] = f"http://localhost:8000/fhir/v1/export-status/{job_id}"
    return {}

@app.get("/fhir/v1/export-status/{job_id}")
def check_export_status(job_id: str):
    job = JOBS_DB.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")

    download_url = f"http://localhost:8000/fhir/v1/download/{job['file_name']}"
    
    return {
        "transactionTime": "2026-10-03T12:00:00Z",
        "request": "http://localhost:8000/fhir/v1/Patient/$export",
        "requiresAccessToken": False,
        "output": [{"type": "Patient", "url": download_url}],
        "error": []
    }

@app.get("/fhir/v1/download/{file_name}")
def download_ndjson(file_name: str, request: Request):
    file_path = os.path.join(EXPORT_DIR, file_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    file_size = os.path.getsize(file_path)
    range_header = request.headers.get("range")

    start_byte, end_byte = 0, file_size - 1
    if range_header:
        byte_range = range_header.replace("bytes=", "").split("-")
        start_byte = int(byte_range[0])
        if byte_range[1]:
            end_byte = int(byte_range[1])

    chunk_len = (end_byte - start_byte) + 1

    def stream_bytes():
        with open(file_path, "rb") as f:
            f.seek(start_byte)
            yield f.read(chunk_len)

    headers = {
        "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_len),
        "Content-Type": "application/fhir+ndjson",
    }
    
    return StreamingResponse(
        stream_bytes(),
        status_code=status.HTTP_206_PARTIAL_CONTENT if range_header else status.HTTP_200_OK,
        headers=headers
    )


# ==========================================
# PART 2: C-CDA XML PARSER & VALIDATOR
# ==========================================

class MedicalItem(BaseModel):
    id: Optional[str] = None
    code: Optional[str] = None
    display: Optional[str] = None
    status: Optional[str] = None

class CCDAResponse(BaseModel):
    problems: List[MedicalItem] = []

@app.post("/ccda/v1/parse")
async def parse_ccda(request: Request):
    raw_xml = await request.body()
    try:
        root = etree.fromstring(raw_xml)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid XML format")

    # Simple Schematron-style validation check
    ns = {"cda": "urn:hl7-org:v3"}
    template_check = root.xpath("//cda:templateId[@root='2.16.840.1.113883.10.20.22.1.1']", namespaces=ns)
    if not template_check:
        return JSONResponse(status_code=422, content={"error": "Missing valid C-CDA templateId root."})

    # Parsing problems from XML
    problems = []
    entries = root.xpath("//cda:section[cda:templateId[@root='2.16.840.1.113883.10.20.22.2.5.1']]//cda:act", namespaces=ns)
    
    for entry in entries:
        id_val = entry.xpath("./cda:id/@root", namespaces=ns)
        code_val = entry.xpath("./cda:code/@code", namespaces=ns)
        display_val = entry.xpath("./cda:code/@displayName", namespaces=ns)
        status_val = entry.xpath("./cda:statusCode/@code", namespaces=ns)

        problems.append(MedicalItem(
            id=id_val[0] if id_val else None,
            code=code_val[0] if code_val else None,
            display=display_val[0] if display_val else None,
            status=status_val[0] if status_val else None
        ))

    return CCDAResponse(problems=problems)