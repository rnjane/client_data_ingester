from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Numeric, TIMESTAMP, func
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Client(Base):
    __tablename__ = 'clients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String(255), nullable=False)
    sign_up_dt = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    address = Column(String(512))
    active = Column(Boolean, nullable=False, server_default='1')

    users = relationship('User', back_populates='client', cascade='all, delete-orphan')


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey('clients.id'), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    created_on = Column(DateTime, nullable=False)
    password_hash = Column(String(255), nullable=False)
    active = Column(Boolean, nullable=False, server_default='1')
    session_token = Column(String(255), nullable=True)
    last_login = Column(DateTime, nullable=True)

    client = relationship('Client', back_populates='users')


class ClientProduct(Base):
    __tablename__ = 'client_products'

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey('clients.id'), nullable=False)
    sku = Column(String(100), nullable=False)
    remote_id = Column(String(100))
    brand = Column(String(100))
    title = Column(String(255))
    last_changed_on = Column(TIMESTAMP)
    stock_quantity = Column(Integer)
    active = Column(Boolean, nullable=False, server_default='1')
    max_price = Column(Numeric(12, 2))
    min_price = Column(Numeric(12, 2))
    reference_price = Column(Numeric(12, 2))

    client = relationship('Client', back_populates='products')
