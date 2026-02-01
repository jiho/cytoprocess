import logging
import subprocess
from pathlib import Path
from cytoprocess.utils import get_sample_files, ensure_project_dir, log_command_success, setup_file_logging, log_command_start


def run(ctx, project, force=False):
    logger = logging.getLogger("convert")
    setup_file_logging(logger, project)

    log_command_start(logger, "Converting .cyz files", project)
    
    if force:
        logger.debug("Force flag enabled: existing .json files will be overwritten")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Get .cyz files from raw directory
    cyz_files = get_sample_files(project, kind="cyz", ctx=ctx)
    if (not cyz_files):
        return
    
    # Get the path to Cyz2Json binary
    logger.debug("Getting path to Cyz2Json binary")
    from cytoprocess.commands import install
    try:
        cyz2json_path = install._check_or_get_cyz2json()
    except Exception as e:
        logger.error(f"Failed to get Cyz2Json binary: {e}")
        raise
        
    # Create processed directory if it doesn't exist
    converted_dir = ensure_project_dir(project, "converted")

    # Convert each .cyz file
    for cyz_file in cyz_files:
        json_file = converted_dir / (cyz_file.stem + ".json")
        
        # Skip if JSON file already exists and force is not enabled
        if json_file.exists() and not force:
            logger.info(f"Skipping '{cyz_file.name}', json file already exists (use --force to overwrite)")
            continue
        
        logger.info(f"Converting 'raw/{cyz_file.name}' to 'converted/{json_file.name}'")
        
        try:
            # Build and log the command
            command = [cyz2json_path, str(cyz_file), "--raw", "--output", str(json_file)]
            logger.debug(f"Running command: {' '.join(command)}")
            
            # Run Cyz2Json to convert the file
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True
            )
            logger.debug(f"Successfully converted '{cyz_file.name}'")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to convert '{cyz_file.name}': {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Error converting '{cyz_file.name}': {e}")
            raise
    
    log_command_success(logger, "Convert")
