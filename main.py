import uvicorn
from fastapi import FastAPI

from pipeline_orchestrator.api import build_router
from pipeline_orchestrator.service import bootstrap_demo

app = FastAPI(title="Pipeline Run Orchestrator (local bootstrap)")

_services = bootstrap_demo()
app.include_router(build_router(_services.orchestrator))


@app.get("/")
def read_root():
    return {"Hello": "World", "service": "pipeline-orchestrator"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
