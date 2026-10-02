import os

from sqlalchemy import Integer, String, UniqueConstraint, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://ticket:ticket@127.0.0.1:5432/ticketdrop"
)

engine = create_async_engine(DATABASE_URL, pool_size=20, max_overflow=20)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Sale(Base):
    __tablename__ = "sales"
    # Last line of defence: the DB physically cannot store a double sale.
    __table_args__ = (UniqueConstraint("event_id", "seat_id", name="uq_sale_event_seat"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(Integer)
    seat_id: Mapped[int] = mapped_column(Integer)
    user_id: Mapped[str] = mapped_column(String(64))


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def reset_sales() -> None:
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE sales RESTART IDENTITY"))


__all__ = ["Session", "Sale", "init_db", "reset_sales", "AsyncSession"]
