from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from mply_ingester.config import ConfigBroker
from mply_ingester.web.api import auth, products

def make_app(config_broker: ConfigBroker) -> FastAPI:
    app = FastAPI(title="Client Data Ingester")

    app.dependency_overrides[ConfigBroker] = lambda: config_broker

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
    app.include_router(products.router, prefix="/products", tags=["products"])

    return app

def create_app():
    return make_app(ConfigBroker([]))

if __name__ == "__main__":
    import uvicorn
    # Pass the app as an import string for reload to work properly
    uvicorn.run(
        "mply_ingester.web.app:create_app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        factory=True,
    )
