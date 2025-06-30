from datetime import datetime
from fastapi import APIRouter, HTTPException, status, Response
from fastapi.security import HTTPBasicCredentials
from pydantic import BaseModel
import secrets
import bcrypt
from mply_ingester.backend.web.dependencies import DbSession, CurrentUser
from mply_ingester.backend.db.models import User


router = APIRouter()


class LoginResponse(BaseModel):
    email: str
    name: str


@router.post("/login", response_model=LoginResponse)
async def login(
    credentials: HTTPBasicCredentials,
    db: DbSession,
    response: Response
):
    user = db.query(User).filter(
        User.email == credentials.username,
        User.active == True
    ).first()
    
    if not user or not bcrypt.checkpw(
        credentials.password.encode('utf-8'),
        user.password_hash.encode('utf-8')
    ):
        # TODO: Vulnerable to timing attack here
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Generate session token
    session_token = secrets.token_urlsafe(32)
    
    # Update user with session token and last login
    user.session_token = session_token
    user.last_login = datetime.utcnow()
    db.commit()
    
    # Set secure cookie
    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=True,
        secure=True,  # Enable in production
        samesite="lax",
        max_age=7 * 24 * 3600  # 7 days
    )
    
    return LoginResponse(
        email=user.email,
        name=user.name
    )

@router.post("/logout")
async def logout(
    current_user: CurrentUser,
    db: DbSession,
    response: Response
):
    # Clear session token in database
    current_user.session_token = None
    db.commit()
    
    # Clear cookie
    response.delete_cookie(
        key="session_token",
        httponly=True,
        secure=True,
        samesite="lax"
    )
    
    return {"message": "Successfully logged out"}
