import logging
import os


def run(ctx, directory):
    logger = logging.getLogger("cytoprocess.create")
    logger.info(f"create project {directory}")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Create the main directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)
    
    # List of subdirectories to create
    subdirectories = ["raw", "converted", "work", "config", "meta", "ecotaxa"]
    
    # Create each subdirectory
    for subdir in subdirectories:
        subdir_path = os.path.join(directory, subdir)
        os.makedirs(subdir_path, exist_ok=True)
        logger.debug(f"Created subdirectory: {subdir_path}")
