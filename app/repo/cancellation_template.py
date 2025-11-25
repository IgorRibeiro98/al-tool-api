"""Cancellation templates deprecated.

Templates were removed from the application in favor of standalone
`cancellation_configs`. This module remains as a placeholder to avoid
ImportError in transitional states; its functions will raise an error if used.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _err(*_args, **_kwargs):
    raise RuntimeError("cancellation templates removed: use cancellation_configs instead")


def save_cancellation_template(*args: Any, **kwargs: Any) -> int:
    return _err()


def get_cancellation_template(*args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
    return _err()


def list_cancellation_templates(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    return _err()


__all__ = ["save_cancellation_template", "get_cancellation_template", "list_cancellation_templates"]
