import os
import logging

from pathlib import Path
from dotenv import load_dotenv

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

def get_bootstrap_server() -> str:
    """
    Получаем bootstrap_server.

    :return: URL: str.
    """
    # Получаем секреты
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError(f"Environment file {env_path} not found")
    # URL schema registry
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    logging.info(f"Bootstrap server: {bootstrap_server}")
    return bootstrap_server


def get_users_coordinates_topic() -> str:
    """
    Получаем users_coordinates topic.

    :return: URL: str.
    """
    # Получаем секреты
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError(f"Environment file {env_path} not found")
    # URL schema registry
    users_coordinates_topic = os.getenv("KAFKA_USERS_COORDINATES_TOPIC")
    logging.info(f"User coordinates topic: {users_coordinates_topic}")
    return users_coordinates_topic


if __name__ == "__main__":
    bootstrap_serv = get_bootstrap_server()
    users_coord_topic = get_users_coordinates_topic()