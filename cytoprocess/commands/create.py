import logging
import os
import shutil
from pathlib import Path

from cytoprocess.utils import ensure_project_dir


def run(ctx, project):
    logger = logging.getLogger("cytoprocess.create")
    logger.info(f"Create project {project}")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Create the main directory if it doesn't exist
    if (Path(project).exists()):
        logger.info(f"Project directory {project} already exists.")
    else:
        logger.info(f"Creating project directory {project}.")
        ensure_project_dir(project, "")
    
    # List of subdirectories to create
    subdirectories = ["raw", "converted", "work", "config", "meta", "ecotaxa"]
    
    # Create each subdirectory
    for subdir in subdirectories:
        subdir_path = ensure_project_dir(project, subdir)
        logger.debug(f"Created subdirectory: {subdir_path}")
    
    # Copy metadata configuration template to config directory
    template_file = Path(__file__).parent.parent / "templates" / "metadata_config.yaml"
    dest_file = Path(project) / "config" / "metadata_config.yaml"
    if not dest_file.exists():
        logger.debug(f"Copying metadata configuration template to {dest_file}")
        shutil.copy2(template_file, dest_file)
    else:
        logger.debug(f"Metadata configuration file already exists at {dest_file}")

    logger.info("Project creation completed successfully")
