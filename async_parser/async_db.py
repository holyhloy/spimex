from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncAttrs
from sqlalchemy.orm import declarative_base, sessionmaker, DeclarativeBase
from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER


DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

class BaseModel(AsyncAttrs, DeclarativeBase):
    __abstract__ = True

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, echo=True, future=True)
Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def create_db():
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.create_all)
