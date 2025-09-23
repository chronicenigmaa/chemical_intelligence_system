from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import settings
from app.db.models import Base

def _to_async_url(url_str: str) -> str:
    url = make_url(url_str)
    # If already async, keep it
    if url.drivername == "postgresql+asyncpg":
        return str(url)
    # If plain or psycopg2/psycopg, coerce to asyncpg
    if url.drivername.startswith("postgresql"):
        url = url.set(drivername="postgresql+asyncpg")
        return str(url)
    # Fallback: assume Postgres and force asyncpg
    return f"postgresql+asyncpg://{url.username}:{url.password}@{url.host}:{url.port or 5432}/{url.database}"

ASYNC_DATABASE_URL = _to_async_url(settings.DATABASE_URL)

engine = create_async_engine(ASYNC_DATABASE_URL, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

async def init_models():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
