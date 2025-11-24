from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.reconciliation import router as reconciliation_router
from app.api.datasets import router as datasets_router
from app.api.preprocess import router as preprocess_router
from app.api.cancellation import router as cancellation_router
from app.api.reversal import router as reversal_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # ou liste origens específicas
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok", "message": "FastAPI rodando"}


# include routers implemented in app/api
app.include_router(reconciliation_router)
app.include_router(datasets_router)
app.include_router(preprocess_router)
app.include_router(cancellation_router)
app.include_router(reversal_router)
