import logging
import pandas as pd
from pathlib import Path
from cytoprocess.utils import ensure_project_dir, raiseCytoError, setup_logging, log_command_start, log_command_success


DEFAULT_EXTRA_FIELDS = "object_lon,object_lat,object_date,object_time,object_depth_min,object_depth_max,object_lon_end,object_lat_end"


def run(ctx, project, extra_fields=DEFAULT_EXTRA_FIELDS):
    """
    List all samples in a project and create/update the samples.csv metadata file.
    
    This creates a CSV file in the meta directory with the list of all samples
    (based on converted .json files), which can be used to add custom metadata.
    
    Args:
        ctx: Click context
        project: Path to the project directory
        extra_fields: Comma-separated string of extra field names to add as columns
    """
    logger = setup_logging(command="list", project=project, debug=ctx.obj["debug"])

    log_command_start(logger, "Listing samples", project)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))

    # Parse extra fields
    if extra_fields:
        extra_field_list = [f.strip() for f in extra_fields.split(',') if f.strip()]
    else:
        extra_field_list = []
    logger.debug(f"Extra fields: {extra_field_list}")

    # Check that the 'converted' directory exists
    converted_dir = Path(project) / "converted"
    if not converted_dir.exists():
        raiseCytoError(f"Converted directory not found: '{converted_dir}', run 'cytoprocess convert {project}' first.", logger)

    # Create metadata CSV with sample information   
    meta_dir = ensure_project_dir(project, "meta")
    meta_file = meta_dir / "samples.csv"
    
    # Create DataFrame from converted files
    converted_files = list(converted_dir.glob("*.json"))
    if not converted_files:
        raiseCytoError(f"No .json files found in '{converted_dir}'", logger)
        return
    
    # Create 'samples' DataFrame
    samples = pd.DataFrame({
        'sample_id': [f.stem for f in converted_files]
    })
    for field in extra_field_list:
        samples[field] = None
    
    # Print sample IDs to console
    print(f"{len(samples)} samples found:")
    for sample_id in samples['sample_id']:
        print(f"   {sample_id}")

    # Read existing metadata if it exists, otherwise create new
    update_meta_file = True
    if meta_file.exists():
        existing_samples = pd.read_csv(meta_file)
        
        # Detect which samples are new
        new_samples = samples[~samples['sample_id'].isin(existing_samples['sample_id'])]
        
        # If there are no new samples, just ensure extra fields are present
        if new_samples.empty:
            missing_fields = [f for f in extra_field_list if f not in existing_samples.columns]
            if not missing_fields:
                logger.info(f"No new samples or fields to add to '{meta_file}'")
                # In that case do not even rewrite the file
                update_meta_file = False
            else:
                final_df = existing_samples
                logger.info(f"Adding {len(missing_fields)} new field(s) to '{meta_file}'")
                for field in missing_fields:
                    logger.debug(f"Adding new column '{field}' to '{meta_file}'")
                    final_df[field] = None
                    
        # If there are new samples, append them
        else:
            # Detect potentially missing fields in existing samples to inform the user about it
            missing_fields = [f for f in extra_field_list if f not in existing_samples.columns]
            logger.info(f"Adding {len(new_samples)} new sample(s)" + (f" and {len(missing_fields)} new field(s)" if missing_fields else "") + f" to '{meta_file}'")
            logger.debug(f"Missing samples: {new_samples['sample_id'].tolist()}")
            logger.debug(f"Missing fields: {missing_fields}")
            final_df = pd.concat([existing_samples, new_samples], ignore_index=True)
   
    else:
        final_df = samples
        logger.info(f"Created file '{meta_file}' with {len(samples)} sample(s) and {samples.shape[1]-1} field(s), you can now add custom metadata.")
    
    # Still save if we added new columns
    if update_meta_file:
        final_df.to_csv(meta_file, index=False)
 
    log_command_success(logger, "List samples")
