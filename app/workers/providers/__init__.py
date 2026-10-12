from app.workers.providers.base import TranslationItem, TranslationProvider
from app.workers.providers.copy_english import CopySourceProvider

_REGISTRY: dict[str, type[TranslationProvider]] = {
    CopySourceProvider.name: CopySourceProvider,
}


def register_provider(cls: type[TranslationProvider]) -> type[TranslationProvider]:
    """Plug in a real MT/LLM provider: decorate the class, then set TRANSLATION_PROVIDER=<name>."""
    _REGISTRY[cls.name] = cls
    return cls


def get_provider(name: str) -> TranslationProvider:
    try:
        return _REGISTRY[name]()
    except KeyError:
        raise ValueError(f"unknown translation provider '{name}' (known: {sorted(_REGISTRY)})") from None


def provider_names() -> list[str]:
    return sorted(_REGISTRY)


__all__ = ["TranslationItem", "TranslationProvider", "register_provider", "get_provider", "provider_names"]
