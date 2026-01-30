import logging
import subprocess
from pathlib import Path


def run(ctx, project, force=False):
    logger = logging.getLogger("cytoprocess.convert")
    logger.info(f"Converting all .cyz files in project={project}")
    
    # Get sample from context
    sample = getattr(ctx, "obj", {}).get("sample")
    if sample:
        logger.debug(f"Limiting conversion to sample: {sample}")
    if force:
        logger.debug("Force flag enabled - existing JSON files will be overwritten")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Define raw and converted directories
    raw_dir = Path(project) / "raw"
    converted_dir = Path(project) / "converted"
        
    # Check if raw directory exists
    logger.debug(f"Checking for raw directory at {raw_dir}")
    if not raw_dir.exists():
        logger.warning(f"Raw directory ('{raw_dir}') does not exist. Are you sure this is a valid project directory?")
        return
    
    # List all .cyz files in raw directory
    logger.debug(f"Listing .cyz files in {raw_dir}")
    cyz_files = list(raw_dir.glob("*.cyz"))

    if not cyz_files:
        logger.info(f"No .cyz files found in {raw_dir}")
        return
        
    # Filter by sample if specified
    if sample:
        cyz_files = [f for f in cyz_files if f.stem == sample]
        if not cyz_files:
            logger.info(f"No .cyz file found for sample '{sample}' in {raw_dir}")
            return
        logger.info(f"Found file 'raw/{cyz_files[0].name}'")
    else:
        logger.info(f"Found {len(cyz_files)} .cyz files to convert")
    
    # Get the path to Cyz2Json binary
    logger.debug("Getting path to Cyz2Json binary")
    from cytoprocess.commands import install
    try:
        cyz2json_path = install._check_or_get_cyz2json()
    except Exception as e:
        logger.error(f"Failed to get Cyz2Json binary: {e}")
        raise
    
    logger.debug(f"Using Cyz2Json at {cyz2json_path}")
    
    # Create processed directory if it doesn't exist
    logger.debug(f"Checking if directory {converted_dir} exists and creating if necessary")
    converted_dir.mkdir(parents=True, exist_ok=True)

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
            logger.error(f"Failed to convert {cyz_file.name}: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Error converting {cyz_file.name}: {e}")
            raise
    
    logger.info("Convert operation completed successfully")
