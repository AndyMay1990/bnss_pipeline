# Configuration for the BNSS Pipeline

from pydantic_settings import BaseSettings

class Config(BaseSettings):
    source: str  # e.g., 'cytrain' or 'ncrb'
    rate_limit: int  # Maximum requests per minute
    user_agent: str  # Custom user agent string
    output_dir: str  # Directory for output files

    class Config:
        env_file = ".env"  # Load environment variables from a .env file

# Example usage:
# config = Config(source="cytrain", rate_limit=60, user_agent="MyAgent", output_dir="./output")