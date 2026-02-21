from pydantic_settings import BaseSettings

class Setting(BaseSettings):
    ACCESS_KEY:str
    SECRET_ACCESS_KEY:str
    GEMINI_API_KEY:str
    REDIS_HOST:str
    POSTGRES_HOST:str
    POSTGRES_PORT:str
    POSTGRES_USER:str
    POSTGRES_PASSWORD:str
    POSTGRES_DB:str
    
    class Config:
        env_file = ".env"
    
settings = Setting()