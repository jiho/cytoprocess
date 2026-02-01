import logging
from pathlib import Path
from cytoprocess.utils import setup_file_logging, log_command_start, log_command_success


def run(ctx, project):
    logger = logging.getLogger("cleanup")
    setup_file_logging(logger, project)

    log_command_start(logger, "Cleaning up intermediate files", project)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Define converted directory
    converted_dir = Path(project) / "converted"
    
    # Check if converted directory exists
    logger.debug(f"Checking if converted directory exists at {converted_dir}")
    if not converted_dir.exists():
        logger.warning(f"Converted directory does not exist: {converted_dir}")
        return
    
    # List all .json files in converted directory
    logger.debug(f"Listing .json files in {converted_dir}")
    json_files = list(converted_dir.glob("*.json"))
    
    if not json_files:
        logger.info(f"No .json files found in '{converted_dir}'")
        return
    
    logger.info(f"Found {len(json_files)} .json files to delete")
    
    # Delete each .json file
    for json_file in json_files:
        try:
            logger.debug(f"Deleting '{json_file.name}'")
            json_file.unlink()
            logger.info(f"Deleted '{json_file.name}'")
        except Exception as e:
            logger.error(f"Failed to delete '{json_file.name}': {e}")
            raise
    
    log_command_success(logger, "Cleanup")
