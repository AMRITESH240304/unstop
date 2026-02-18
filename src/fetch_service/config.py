from pydantic_settings import BaseSettings

class Setting(BaseSettings):
    ACCESS_KEY:str
    SECRET_ACCESS_KEY:str
    GEMINI_API_KEY:str
    REDIS_HOST:str
    
    class Config:
        env_file = ".env"
    
settings = Setting()