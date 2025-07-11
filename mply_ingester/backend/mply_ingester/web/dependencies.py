from typing import Annotated, Generator
from fastapi import Depends, HTTPException, status, Cookie, Request
from sqlalchemy.orm import Session
from mply_ingester.config import ConfigBroker
from mply_ingester.db.models import User, Client


async def get_db_session(config_broker: ConfigBroker = Depends()) -> Generator[Session, None, None]:
    db = config_broker.get_session()
    try:
        yield db
    finally:
        db.close()

async def get_current_user(
    request: Request,
    session_token: Annotated[str | None, Cookie()] = None,
    db: Session = Depends(get_db_session)
) -> User:
    if not session_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated" + str(request.cookies)
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
LoggedInUser = Annotated[User, Depends(get_current_user)]
LoggedInClient = Annotated[Client, Depends(get_current_client)]
DbSession = Annotated[Session, Depends(get_db_session)]
