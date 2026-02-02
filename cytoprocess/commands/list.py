import logging
import pandas as pd
from pathlib import Path
from cytoprocess.utils import ensure_project_dir, setup_file_logging, log_command_start, log_command_success


def run(ctx, project):
    """
    List all samples in a project and create/update the samples.csv metadata file.
    
    This creates a CSV file in the meta directory with the list of all samples
    (based on converted .json files), which can be used to add custom metadata.
    """
    logger = logging.getLogger("list")
    setup_file_logging(logger, project)

    log_command_start(logger, "Listing samples", project)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))

    # Check that the 'converted' directory exists
    converted_dir = Path(project) / "converted"
    if not converted_dir.exists():
        logger.error(f"Converted directory not found: '{converted_dir}'. Run convert first.")
        raise FileNotFoundError(f"Converted directory not found: '{converted_dir}'")

    # Create metadata CSV with sample information   
    meta_dir = ensure_project_dir(project, "meta")
    meta_file = meta_dir / "samples.csv"
    
    # Create DataFrame from converted files
    converted_files = list(converted_dir.glob("*.json"))
    if not converted_files:
        logger.warning(f"No .json files found in '{converted_dir}'")
        return
    
    samples = pd.DataFrame({
        'sample_id': [f.stem for f in converted_files]
    })
    
    # Print sample IDs to console
    print(f"{len(samples)} samples found:")
    for sample_id in samples['sample_id']:
        print(f"   {sample_id}")

    # Read existing metadata if it exists, otherwise create new
    if meta_file.exists():
        existing_df = pd.read_csv(meta_file)
        # Only append rows that don't already exist
        samples = samples[~samples['sample_id'].isin(existing_df['sample_id'])]
        if not samples.empty:
            combined_df = pd.concat([existing_df, samples], ignore_index=True)
            combined_df.to_csv(meta_file, index=False)
            logger.info(f"Added {len(samples)} new sample(s) to '{meta_file}'")
        else:
            logger.info(f"No new samples to add to '{meta_file}' (it already contains {len(existing_df)} sample(s))")
    else:
        samples.to_csv(meta_file, index=False)
        logger.info(f"Created file '{meta_file}' with {len(samples)} sample(s), you can now add custom metadata.")

    log_command_success(logger, "List samples")
