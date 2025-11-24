from app.core.config import settings


def test_settings_defaults():
    assert settings.DB_PATH == "data/app.db"
    assert settings.STORAGE_PATH == "storage"
