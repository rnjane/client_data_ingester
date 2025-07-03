from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from mply_ingester.config import ConfigBroker
from mply_ingester.web.api import auth

def make_app(config_broker: ConfigBroker) -> FastAPI:
    app = FastAPI(title="Client Data Ingester")

    app.dependency_overrides[ConfigBroker] = lambda: config_broker

    # Include routers
    app.include_router(auth.router, prefix="/auth", tags=["auth"])

    return app
