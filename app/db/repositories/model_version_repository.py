from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import SportType
from app.db.models.model_version import ModelVersion


class ModelVersionRepository:
    async def get_active_by_name(
        self,
        session: AsyncSession,
        sport: SportType,
        model_key: str,
        version_name: str,
    ) -> ModelVersion | None:
        """Return active model version by identifiers or None."""
        stmt = (
            select(ModelVersion)
            .where(ModelVersion.sport == sport)
            .where(ModelVersion.model_key == model_key)
            .where(ModelVersion.version_name == version_name)
            .where(ModelVersion.is_active.is_(True))
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_version_name(
        self,
        session: AsyncSession,
        sport: SportType,
        version_name: str,
    ) -> ModelVersion | None:
        """Return model version for a sport by version_name or None."""
        stmt = select(ModelVersion).where(ModelVersion.sport == sport).where(ModelVersion.version_name == version_name)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

