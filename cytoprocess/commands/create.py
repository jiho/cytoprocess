import logging
import os
from pathlib import Path


def run(ctx, project):
    logger = logging.getLogger("cytoprocess.create")
    logger.info(f"Create project {project}")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Create the main directory if it doesn't exist
    if (Path(project).exists()):
        logger.info(f"Project directory {project} already exists." )
    os.makedirs(project, exist_ok=True)
    
    # List of subdirectories to create
    subdirectories = ["raw", "converted", "work", "config", "meta", "ecotaxa"]
    
    # Create each subdirectory
    for subdir in subdirectories:
        subdir_path = os.path.join(project, subdir)
        os.makedirs(subdir_path, exist_ok=True)
        logger.debug(f"Created subdirectory: {subdir_path}")
