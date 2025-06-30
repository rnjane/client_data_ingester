from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mply_ingester.backend.web.api import auth

app = FastAPI(title="Client Data Ingester")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Adjust based on your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/auth", tags=["auth"])
