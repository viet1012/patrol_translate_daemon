"""Configuration shared by the legacy worker modules.

Values come from the repository's .env file.  The aliases preserve
compatibility with the previous DB_USERNAME / LLM_URL naming.
"""

import os

from dotenv import load_dotenv


load_dotenv(override=True)


class Config:
    # SQL Server
    DB_SERVER = os.getenv("DB_SERVER")
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_USERNAME = os.getenv("DB_USER") or os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")

    # Source-table columns
    ID_COLUMN = os.getenv("ID_COLUMN", "id")
    SOURCE_COLUMN = os.getenv("SOURCE_COLUMN", "comment")
    CREATED_COLUMN = os.getenv("CREATED_COLUMN", "createdAt")
    EDITED_COLUMN = os.getenv("EDITED_COLUMN", "edit_date")
    JAPANESE_COLUMN = os.getenv("JAPANESE_COLUMN", "comment_japanese")
    AI_TRANSLATE_UPDATE_COLUMN = os.getenv(
        "AI_TRANSLATE_UPDATE_COLUMN", "ai_translate_update_at"
    )

    # Local LLM.  LM_* is the current name; LLM_* remains supported.
    LLM_URL = os.getenv("LM_URL") or os.getenv("LLM_URL")
    LLM_MODEL = os.getenv("LM_MODEL") or os.getenv("LLM_MODEL")

    # Service settings
    CHECK_INTERVAL = int(
        os.getenv("POLL_INTERVAL_SECONDS") or os.getenv("CHECK_INTERVAL", "5")
    )
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
