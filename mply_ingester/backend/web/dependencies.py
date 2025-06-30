from typing import Annotated
from fastapi import Depends, HTTPException, status, Cookie
from sqlalchemy.orm import Session
from mply_ingester.backend.config import ConfigBroker
from mply_ingester.backend.db.models import User, Client

config = ConfigBroker(['config.py'])  # You'll need to adjust this path based on your setup

def get_db_session() -> Session:
    db = config.get_session()
    try:
        yield db
    finally:
        db.close()

async def get_current_user(
    session_token: Annotated[str | None, Cookie()] = None,
    db: Session = Depends(get_db_session)
) -> User:
    if not session_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    user = db.query(User).filter(
        User.session_token == session_token,
        User.active == True
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token"
        )
    
    return user

async def get_current_client(
    current_user: Annotated[User, Depends(get_current_user)]
) -> Client:
    return current_user.client

# Create type aliases for cleaner dependency injection
CurrentUser = Annotated[User, Depends(get_current_user)]
CurrentClient = Annotated[Client, Depends(get_current_client)]
DbSession = Annotated[Session, Depends(get_db_session)]
