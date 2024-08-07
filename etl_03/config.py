import os 
# from pydantic_settings import BaseSettings
# from pydantic import BaseSettings
from dotenv import load_dotenv 

load_dotenv(override=True) 

class CustomSettings():
    # SERVER 1
    SERVER_IP_1: str = os.environ["SERVER_IP_1"]
    SERVER_USERNAME_1: str = os.environ["SERVER_USERNAME_1"]
    SERVER_PASSWORD_1: str = os.environ["SERVER_PASSWORD_1"]
    SERVER_PORT_1: str = os.environ["SERVER_PORT_1"]

    # SERVER 2
    SERVER_IP_2: str = os.environ["SERVER_IP_2"]
    SERVER_USERNAME_2: str = os.environ["SERVER_USERNAME_2"]
    SERVER_PASSWORD_2: str = os.environ["SERVER_PASSWORD_2"]
    SERVER_PORT_2: str = os.environ["SERVER_PORT_2"]

    # ORACLE DB
    ORACLE_HOST: str = os.environ["ORACLE_HOST"]
    ORACLE_PORT: str = os.environ["ORACLE_PORT"]
    ORACLE_SID: str = os.environ["ORACLE_SID"]
    ORACLE_USER: str = os.environ["ORACLE_USER"]
    ORACLE_PASSWORD: str = os.environ["ORACLE_PASSWORD"]

settings=CustomSettings()
