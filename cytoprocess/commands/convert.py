import logging
import subprocess
from pathlib import Path
import pandas as pd
from cytoprocess.utils import get_sample_files, ensure_project_dir, log_command_success, setup_file_logging, log_command_start
from cytoprocess.commands import install


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
    
    # Create metadata CSV with sample information
    
    meta_dir = ensure_project_dir(project, "meta")
    meta_file = meta_dir / "samples.csv"
    
    # Create DataFrame from converted files
    converted_files = list(converted_dir.glob("*.json"))
    new_samples = pd.DataFrame({
        'sample_id': [f.stem for f in converted_files]
    })
    
    # Read existing metadata if it exists, otherwise create new
    if meta_file.exists():
        existing_df = pd.read_csv(meta_file)
        # Only append rows that don't already exist
        new_samples = new_samples[~new_samples['sample_id'].isin(existing_df['sample_id'])]
        if not new_samples.empty:
            combined_df = pd.concat([existing_df, new_samples], ignore_index=True)
            combined_df.to_csv(meta_file, index=False)
            logger.info(f"Added {len(new_samples)} new sample(s) to metadata")
    else:
        new_samples.to_csv(meta_file, index=False)
        logger.info(f"Created custom metadata file with {len(new_samples)} sample(s)")

    log_command_success(logger, "Convert")
