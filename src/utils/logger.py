import logging
import os

# --> pour configurer les logs
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOGS_DIR = os.path.join(ROOT_DIR, "logs")

# --> existence du dossier logs
os.makedirs(LOGS_DIR, exist_ok=True)

def setup_logger(name, filename, level=logging.INFO):

    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        return logger

    # --> afichage des logs sous forme de format 
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] (%(filename)s:%(lineno)d): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # --> ficher pour sauvegarder les logs
    log_file_path = os.path.join(LOGS_DIR, filename)
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # --> pour afficher les logs sur la console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# --> separate loggers based on operational domain
pipeline_logger = setup_logger("pipeline", "pipeline.log")
auth_logger = setup_logger("auth", "auth.log")
