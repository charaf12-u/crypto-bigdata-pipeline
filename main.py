from src.ingestion.bronze_ingestion import run_bronze_ingestion
from src.transformation.silver_transformation import run_silver_transformation
from src.modeling.gold_modeling import run_gold_modeling
from src.warehouse.snowflake_loader import run_snowflake_loading
from src.utils.logger import pipeline_logger


def main():
    try:
        pipeline_logger.info("==========================================")
        pipeline_logger.info("Starting CryptoPulse ETL Pipeline locally...")
        pipeline_logger.info("==========================================")
        
        run_bronze_ingestion()
        run_silver_transformation()
        run_gold_modeling()
        run_snowflake_loading()
        
        pipeline_logger.info("==========================================")
        pipeline_logger.info("CryptoPulse ETL Pipeline executed successfully.")
        pipeline_logger.info("==========================================")

    except Exception as error:
        pipeline_logger.error(f"CryptoPulse ETL Pipeline execution failed: {error}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()