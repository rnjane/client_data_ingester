from datetime import datetime, timezone
from typing import Annotated

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, status, Request, Response, Body, Form
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.security import HTTPBasicCredentials
from pydantic import BaseModel, EmailStr, SecretStr
import secrets
from sqlalchemy.orm import Session

from mply_ingester.config import ConfigBroker
from mply_ingester.web.dependencies import DbSession, LoggedInUser, get_db_session
from mply_ingester.db.models import Client, User


router = APIRouter()


class LoginResponse(BaseModel):
    email: str
    full_name: str




class SignupResponse(BaseModel):
    email: str
    full_name: str
    company_name: str


@router.post("/login", response_model=LoginResponse)
async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    response: Response,
    db: Session = Depends(get_db_session),
):
    user = db.query(User).filter(
        User.email == form_data.username.strip(),
        User.active == True
    ).one_or_none()

    if not user or not bcrypt.checkpw(
        form_data.password.encode('utf-8'),
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

    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=True,
        secure=False,  # This app won't go to production
        samesite="lax",
        max_age=7 * 24 * 3600  # 7 days
    )

    return LoginResponse(
        email=user.email,
        full_name=user.full_name
    )


@router.post("/logout")
async def logout(
    current_user: LoggedInUser,
    db: DbSession,
    response: Response
):
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


@router.post("/signup")
async def signup(
    full_name: Annotated[str, Form(min_length=3, max_length=50)],
    email: Annotated[EmailStr, Form()],
    password: Annotated[SecretStr, Form(min_length=8)],
    company_name: Annotated[str, Form(min_length=5)],
    company_address: Annotated[str, Form(min_length=8)],
    db: Session = Depends(get_db_session)
) -> SignupResponse:
    user_email = email.strip()
    if db.query(User).filter(User.email == user_email).first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    client = Client(company_name=company_name, address=company_address, active=True)
    db.add(client)
    db.flush()  # To get client_id

    password_hash = bcrypt.hashpw(password.get_secret_value().encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    user = User(
        client_id=client.id,
        email=user_email,
        full_name=full_name,
        created_on=datetime.now(tz=timezone.utc),
        password_hash=password_hash,
        active=True
    )
    db.add(user)
    db.commit()
    
    return SignupResponse(email=user.email, full_name=user.full_name, company_name=client.company_name)

