from fastapi import APIRouter, Depends, Form, HTTPException, Query, UploadFile, File, Body
from mply_ingester.config import ConfigBroker
from sqlalchemy.orm import Session
from typing import Annotated, List, Optional

from sqlalchemy import or_, case, func

from mply_ingester.web.dependencies import DbSession, LoggedInClient, LoggedInUser, get_db_session
from mply_ingester.db.models import ClientProduct
from mply_ingester.ingestion.base import ParserConfig, IngestionReport
from mply_ingester.ingestion.service import DataIngestionService
from pydantic import BaseModel

router = APIRouter()

class ClientProductOut(BaseModel):
    id: int
    client_id: int
    sku: str
    remote_id: Optional[str]
    brand: Optional[str]
    title: Optional[str]
    last_changed_on: Optional[str]
    stock_quantity: Optional[int]
    active: bool
    max_price: Optional[float]
    min_price: Optional[float]
    reference_price: Optional[float]

    class Config:
        orm_mode = True

@router.get("/list", response_model=List[ClientProductOut])
async def list_client_products(
    db: DbSession,
    current_user: LoggedInUser,
    s: Annotated[int, Query(ge=0, title="Offset")] = 0,
    l: Annotated[int, Query(ge=1, le=50, title= "Limit")] = 5,
    q: Annotated[str, Query(title="Search query")] = None
):

    offset = s
    limit = l
    query = db.query(ClientProduct).filter(ClientProduct.client_id == current_user.client_id)

    if q:
        # Search in title, remote_id, and sku
        search_filter = or_(
            ClientProduct.title.ilike(f"%{q}%"),
            ClientProduct.remote_id.ilike(f"%{q}%"),
            ClientProduct.sku.ilike(f"%{q}%")
        )
        query = query.filter(search_filter)

        # Order: exact sku matches first, then by how closely sku matches q, then by sku
        exact_match_case = case(
            (func.lower(ClientProduct.sku) == func.lower(q), 0),
            else_=1
        )
        # For "closeness", order by whether sku startswith q, then by sku
        startswith_case = case(
            (ClientProduct.sku.ilike(f"{q}%"), 0),
            else_=1
        )
        query = query.order_by(
            exact_match_case,
            startswith_case,
            ClientProduct.sku
        )
    else:
        query = query.order_by(ClientProduct.sku)

    products = query.offset(offset).limit(limit).all()
    return products

@router.post("/ingest", response_model=IngestionReport)
async def ingest_client_products(
    parser_config: Annotated[str, Form(...)],
    data_file: Annotated[UploadFile, File(...)],
    db: DbSession,
    current_client: LoggedInClient,
    config_broker: ConfigBroker = Depends(),
    full_update: Annotated[bool, Query(description="Full update mode: any product ingested is active, any absent product is inactive")] = False
):
    """
    Ingest client products. 
    
    When full_update=False (default): Incremental mode - updates existing products or creates new ones
    When full_update=True: Full update mode - any product ingested is active, any absent product is inactive
    """
    try:
        parser_config_obj = ParserConfig.model_validate_json(parser_config)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid parser_config: {e}")
    # Read file content
    file_bytes = await data_file.read()
    # Ingest data
    service = DataIngestionService(config_broker, db, current_client)
    
    if full_update:
        report = service.ingest_data_full_update(parser_config_obj, file_bytes)
    else:
        report = service.ingest_data(parser_config_obj, file_bytes)
    
    return report
