from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.models import Language
from app.schemas import LanguageIn, LanguageOut
from app.services.errors import Conflict

router = APIRouter(prefix="/languages", tags=["languages"])


@router.get("", response_model=list[LanguageOut])
async def list_languages(session: AsyncSession = Depends(get_session)):
    return (await session.scalars(select(Language).order_by(Language.id))).all()


@router.post("", response_model=LanguageOut, status_code=status.HTTP_201_CREATED)
async def add_language(body: LanguageIn, session: AsyncSession = Depends(get_session)):
    """Adding a language is an INSERT - no schema change, no deploy."""
    lang = Language(name=body.name, language_code=body.language_code, is_active=body.is_active, is_default=False)
    session.add(lang)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise Conflict("language_exists", f"language '{body.language_code}' already exists") from None
    return lang
