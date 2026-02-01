import logging
from pathlib import Path
from cytoprocess.utils import setup_file_logging, log_command_start, log_command_success
import shutil

def _remove_directory(directory: Path, logger: logging.Logger) -> bool:
    """Remove a directory and all its contents.
    
    Args:
        directory: Path to the directory to remove
        logger: Logger instance for logging operations
        
    Returns:
        bool: True if directory was removed, False if it didn't exist
    """
    
    logger.debug(f"Checking if '{directory}' exists")
    if not directory.exists():
        logger.info(f"Directory does not exist: '{directory}'")
        return False
    
    try:
        shutil.rmtree(directory)
        logger.info(f"Successfully removed '{directory}'")
        return True
    except Exception as e:
        logger.error(f"Failed to remove '{directory}': {e}")
        raise


def run(ctx, project):
    logger = logging.getLogger("cleanup")
    setup_file_logging(logger, project)

    log_command_start(logger, "Cleaning up intermediate files", project)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Remove directory containing .json files
    # they are large and can be reconverted from .cyz files
    converted_dir = Path(project) / "converted"
    _remove_directory(converted_dir, logger)

    # Remove intermediate storage for metadata
    meta_dir = Path(project) / "meta"
    _remove_directory(meta_dir, logger)

    # Remove directory with individual images
    images_dir = Path(project) / "images"
    _remove_directory(images_dir, logger)
  
    log_command_success(logger, "Cleanup")
