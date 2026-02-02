import logging
import pandas as pd
from pathlib import Path
from cytoprocess.utils import ensure_project_dir, setup_file_logging, log_command_start, log_command_success


def _infer_ecotaxa_type(series):
    """
    Infer EcoTaxa column type from pandas Series.
    
    Returns '[t]' for text columns and '[f]' for numeric columns.
    
    Args:
        series: pandas Series
        
    Returns:
        str: '[t]' or '[f]'
    """
    # Check if column is numeric
    if pd.api.types.is_numeric_dtype(series):
        return '[f]'
    else:
        return '[t]'


def run(ctx, project, force=False):
    logger = logging.getLogger("prepare")
    setup_file_logging(logger, project)
    
    log_command_start(logger, "Preparing EcoTaxa files", project)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))

    # Check that the directory containing intermediate files exists
    work_dir = Path(project) / "work"
    logger.debug(f"Checking {work_dir}")
    if not work_dir.exists():
        message = f"Work directory not found: '{work_dir}',\
            run previous extraction steps first."
        logger.error(message)
        raise FileNotFoundError(message)

    # Read instrument metadata (sample-level)
    # this will serve as the reference regarding which samples to process
    samples_meta_file = work_dir / "sample_metadata_from_instrument.parquet"
    logger.debug(f"Checking {samples_meta_file}")
    if not samples_meta_file.exists():
        message = f"Samples metadata file not found: '{samples_meta_file}',\
            run `cytoprocess extract_meta {project}`."
        logger.error(message)
        raise FileNotFoundError(message)
    
    logger.info(f"Reading reference for samples metadata from '{samples_meta_file}'")
    metadata_df = pd.read_parquet(samples_meta_file)
    
    # Get list of samples to process
    sample = getattr(ctx, "obj", {}).get("sample")
    if sample:
        samples = [sample]
        logger.info(f"Preparing EcoTaxa file for sample: '{sample}'")
    else:
        samples = metadata_df['sample_id'].unique().tolist()
        logger.info(f"Preparing EcoTaxa file for {len(samples)} sample(s)")
    
    # Check that all required input files exist for all requested samples
    logger.debug("Verifying required input files for all requested samples")
    at_least_one_missing_file = False
    for sample_id in samples:
        cytometric_file = work_dir / f"{sample_id}_cytometric_features.parquet"
        if not cytometric_file.exists():
            at_least_one_missing_file = True
            logger.warning(f"Missing cytometric features, run `cytoprocess --sample {sample_id} extract_features {project}`")
        pulses_file = work_dir / f"{sample_id}_pulses.parquet"
        if not pulses_file.exists():
            at_least_one_missing_file = True
            logger.warning(f"Missing pulses summary, run `cytoprocess --sample {sample_id} summarise_pulses {project}`")
        image_features_file = work_dir / f"{sample_id}_image_features.parquet"
        if not image_features_file.exists():
            at_least_one_missing_file = True
        
    if at_least_one_missing_file:
        raise FileNotFoundError(f"Missing input files for some samples. Please run the required extraction steps before preparing EcoTaxa files.")
            logger.warning(f"Missing image features, run `cytoprocess --sample {sample_id} compute_features {project}`")

    # Ensure ecotaxa directory exists
    ecotaxa_dir = ensure_project_dir(project, "ecotaxa")
    
    # Process each sample
    for sample_id in samples:
        output_file = ecotaxa_dir / f"ecotaxa_{sample_id}.tsv"
        
        # Skip if output file exists and force is not set
        if output_file.exists() and not force:
            logger.info(f"Skipping '{sample_id}', ecotaxa_*.tsv file already exists (use --force to overwrite)")
            continue
        
        logger.info(f"Preparing EcoTaxa file for sample '{sample_id}'")
        
        # Get sample metadata
        samples_df = metadata_df[metadata_df['sample_id'] == sample_id]
        # TODO add manual dataframe of metadata from the meta directory
        
        # Read object metadata files for this sample
        cytometric_file = work_dir / f"{sample_id}_cytometric_features.parquet"
        pulses_file = work_dir / f"{sample_id}_pulses.parquet"
        image_features_file = work_dir / f"{sample_id}_image_features.parquet"
        cytometric_df = pd.read_parquet(cytometric_file)
        pulses_df = pd.read_parquet(pulses_file)
        image_features_df = pd.read_parquet(image_features_file)

        # Merge all data
        df = cytometric_df.merge(pulses_df, on=['sample_id', 'object_id'], how='left')
        df = df.merge(image_features_df, on=['sample_id', 'object_id'], how='left')
        df = df.merge(samples_df, on=['sample_id'], how='left')

        logger.debug(f"Found {len(df)} objects for sample '{sample_id}'")

        # Add image filename (img_file_name) based on particle_id
        # Extract particle_id by removing the sample_id prefix from object_id
        df['img_file_name'] = df['object_id'].str.replace(f"{sample_id}_", "", n=1) + ".png"
        
        # Add img_rank (0-based index for multiple images per object)
        # For now, assuming 1 image per object
        df['img_rank'] = 0
                
        # Count columns per prefix and enforce EcoTaxa limits
        cols = df.columns.tolist()
        img_cols = [c for c in cols if c.startswith('img_')]
        object_cols = [c for c in cols if c.startswith('object_')]
        process_cols = [c for c in cols if c.startswith('process_')]
        acq_cols = [c for c in cols if c.startswith('acq_')]
        sample_cols = [c for c in cols if c.startswith('sample_')]

        # Limit object metadata columns to 500
        # NB: since object_id does not count as metadata, this means a maximum of 501 columns
        if len(object_cols) > 501:
            logger.warning(f"Sample '{sample_id}' has {len(object_cols)-1} object metadata columns, truncating to 500 (EcoTaxa limit)")
            object_cols = object_cols[:501]
        # Limit sample, process, and acq columns to 50 columns of metadata each
        if len(sample_cols) > 51:
            logger.warning(f"Sample '{sample_id}' has {len(sample_cols)-1} sample metadata columns, truncating to 50 (EcoTaxa limit)")
            sample_cols = sample_cols[:51]
        if len(process_cols) > 51:
            logger.warning(f"Sample '{sample_id}' has {len(process_cols)-1} process metadata columns, truncating to 50 (EcoTaxa limit)")
            process_cols = process_cols[:51]
        if len(acq_cols) > 51:
            logger.warning(f"Sample '{sample_id}' has {len(acq_cols)-1} acq metadata columns, truncating to 50 (EcoTaxa limit)")
            acq_cols = acq_cols[:51]

        # Order columns for cleanness
        ordered_cols = img_cols + object_cols + process_cols + acq_cols + sample_cols
        sorted_df = df[ordered_cols]
        
        # Create type indicators row
        type_row = {col: _infer_ecotaxa_type(sorted_df[col]) for col in sorted_df.columns}
        type_df = pd.DataFrame([type_row])
        
        # Create the EcoTaxa .tsv file
        # First write headers, then type row, then data        
        with open(output_file, 'w') as f:
            # Write header
            f.write('\t'.join(sorted_df.columns) + '\n')
            # Write type row
            f.write('\t'.join([type_row[col] for col in sorted_df.columns]) + '\n')
            # Write data rows
            sorted_df.to_csv(f, sep='\t', index=False, header=False)
        
        logger.info(f"Saved {sorted_df.shape[1]} fields for {sorted_df.shape[0]} objects to '{output_file}'")
        # TODO add images to a zip file


    log_command_success(logger, "Prepare EcoTaxa files")
