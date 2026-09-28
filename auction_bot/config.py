import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    token: str
    owners: frozenset
    database: str
    channel_id: str = ""
    group_id: str = ""
    mongodb_uri: str = ""
    mongodb_database: str = "authbid_bot"
    org_mongo_uri: str = ""
    org_mongo_database: str = ""
    org_user_collection: str = "users"
    org_user_id_field: str = "_id"
    org_balance_field: str = "coins"
    org_balance_scale: int = 100
    require_wallet: bool = False

    @classmethod
    def from_env(cls):
        load_dotenv()
        token = os.getenv("BOT_TOKEN", "").strip()
        owners = os.getenv("OWNER_IDS", "").split(",")
        if not token or token == "replace_with_BotFather_token":
            raise ValueError("Set BOT_TOKEN in .env (get it from @BotFather).")
        if not all(owner.strip().isdigit() and int(owner) > 0 for owner in owners):
            raise ValueError("OWNER_IDS must contain comma-separated positive Telegram user IDs.")
        for key in ("CHANNEL_ID", "GROUP_ID"):
            value = os.getenv(key, "").strip()
            if value and (not value.lstrip("-").isdigit() or int(value) >= 0):
                raise ValueError(f"{key} must be a negative numeric Telegram chat ID.")
        uri = os.getenv("MONGODB_URI", "").strip()
        if not uri.startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError("Set MONGODB_URI to a MongoDB replica set or Atlas connection string")
        database = os.getenv("MONGODB_DATABASE", "authbid_bot").strip()
        if not database or any(c in database for c in '/\\. "$*<>:|?') or len(database.encode())>63:
            raise ValueError("MONGODB_DATABASE is invalid")
        # One MongoDB connection is used for both databases; only the database
        # and wallet field are separated below.
        org_uri = uri
        org_database = os.getenv("ORG_MONGO_DB", "").strip()
        if not org_database or any(c in org_database for c in '/\\. "$*<>:|?') or len(org_database.encode())>63:
            raise ValueError("ORG_MONGO_DB is invalid")
        org_collection = os.getenv("ORG_USER_COLLECTION", "users").strip()
        org_id_field = os.getenv("ORG_USER_ID_FIELD", "_id").strip()
        org_balance_field = os.getenv("ORG_BALANCE_FIELD", "coins").strip()
        for name, value in (("ORG_USER_COLLECTION", org_collection),
                            ("ORG_USER_ID_FIELD", org_id_field),
                            ("ORG_BALANCE_FIELD", org_balance_field)):
            if not value or not all(part.replace("_", "").isalnum() for part in value.split(".")):
                raise ValueError(f"{name} must be a simple MongoDB field/collection name")
        try:
            org_scale = int(os.getenv("ORG_BALANCE_SCALE", "100"))
        except ValueError:
            raise ValueError("ORG_BALANCE_SCALE must be a positive integer") from None
        if org_scale <= 0:
            raise ValueError("ORG_BALANCE_SCALE must be a positive integer")
        return cls(token, frozenset(int(owner) for owner in owners),
                   os.getenv("DATABASE_PATH", "data/auctions.sqlite3"),
                   os.getenv("CHANNEL_ID", "").strip(), os.getenv("GROUP_ID", "").strip(), uri, database,
                   org_uri, org_database, org_collection, org_id_field, org_balance_field, org_scale, True)
