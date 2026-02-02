import logging
import pandas as pd
import zipfile
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


def run(ctx, project, force=False, only_tsv=False):
    logger = logging.getLogger("prepare")
    setup_file_logging(logger, project)
    
    log_command_start(logger, "Preparing EcoTaxa files", project)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    if only_tsv:
        logger.debug("Only creating TSV files (--only-tsv flag enabled)")


    ## 1. Read reference list of samples ----

    # = the one from the meta/samples.csv file
    samples_file = Path(project) / "meta" / "samples.csv"
    logger.debug(f"Checking '{samples_file}'")
    if not samples_file.exists():
        message = f"Samples file not found: '{samples_file}',\
            run `cytoprocess list {project}`."
        logger.error(message)
        raise FileNotFoundError(message)
    
    logger.info(f"Reading reference samples list from '{samples_file}'")
    samples_df = pd.read_csv(samples_file)
        
    # Get list of samples to prepare (potentially filtered by --sample)
    sample = getattr(ctx, "obj", {}).get("sample")
    if sample:
        samples = [sample]
        logger.info(f"Preparing EcoTaxa file for sample: '{sample}'")
    else:
        samples = samples_df['sample_id'].unique().tolist()
        logger.info(f"Preparing EcoTaxa file for {len(samples)} sample(s)")
    

    ## 2. Check that all required input files exist, for all requested samples ----

    logger.debug("Verifying required input files for all requested samples")

    # Check existence of file with metadata extracted from the .json
    work_dir = Path(project) / "work"
    instrument_meta_file = work_dir / "sample_metadata_from_instrument.parquet"
    if not instrument_meta_file.exists():
        logger.error(f"Missing metadata from the instrument, run `cytoprocess extract_meta {project}`.")
        return

    # If it is present, read it to (1) check that all samples are there and (2) merge it later
    at_least_one_missing = False
    logger.debug(f"Reading instrument metadata from '{instrument_meta_file}'")
    instrument_meta_df = pd.read_parquet(instrument_meta_file)
    
    for sample_id in samples:
        if sample_id not in instrument_meta_df['sample_id'].values:
            logger.warning(f"Missing metadata from the instrument, run `cytoprocess --sample {sample_id} extract_meta {project}`")
            at_least_one_missing = True

        cytometric_file = work_dir / f"{sample_id}_cytometric_features.parquet"
        if not cytometric_file.exists():
            logger.warning(f"Missing cytometric features, run `cytoprocess --sample {sample_id} extract_features {project}`")
            at_least_one_missing = True
        pulses_file = work_dir / f"{sample_id}_pulses.parquet"
        if not pulses_file.exists():
            logger.warning(f"Missing pulses summary, run `cytoprocess --sample {sample_id} summarise_pulses {project}`")
            at_least_one_missing = True
        image_features_file = work_dir / f"{sample_id}_image_features.parquet"
        if not image_features_file.exists():
            logger.warning(f"Missing image features, run `cytoprocess --sample {sample_id} compute_features {project}`")
            at_least_one_missing = True

        images_dir = Path(project) / "images" / sample_id
        if not images_dir.exists():
            logger.warning(f"Images not found, run `cytoprocess --sample {sample_id} extract_images {project}`")
            at_least_one_missing = True
    
    if at_least_one_missing:
        logger.error(f"Missing input for some samples. Please run the required extraction steps before preparing EcoTaxa files.")
        return

    # TODO detect extra samples everywhere


    ## 3. Prepare EcoTaxa .tsv/.zip files ----

    # Ensure ecotaxa directory exists
    ecotaxa_dir = ensure_project_dir(project, "ecotaxa")
    
    # Process each sample
    for sample_id in samples:
        tsv_file = ecotaxa_dir / f"ecotaxa_{sample_id}.tsv"
        zip_file = ecotaxa_dir / f"ecotaxa_{sample_id}.zip"

        # Skip if output file exists and force is not set
        if (tsv_file.exists() and only_tsv and not force) or (zip_file.exists() and not force):
            logger.info(f"Skipping '{sample_id}', ecotaxa_*." + ("tsv" if only_tsv else "zip") + " file already exists (use --force to overwrite)")
            continue
        
        logger.info(f"Preparing '{tsv_file}'")

        ## 3.1 Merge all data for this sample ----
        
        # Get sample-level metadata
        sample_meta = samples_df[samples_df['sample_id'] == sample_id]
        instrument_meta = instrument_meta_df[instrument_meta_df['sample_id'] == sample_id]
        
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
        df = df.merge(sample_meta, on=['sample_id'], how='left')
        df = df.merge(instrument_meta, on=['sample_id'], how='left')

        logger.debug(f"Found {len(df)} objects for sample '{sample_id}'")

        # Add image filename (img_file_name) based on particle_id
        # Extract particle_id by removing the sample_id prefix from object_id
        df['img_file_name'] = df['object_id'].str.replace(f"{sample_id}_", "", n=1) + ".png"
        
        # Add img_rank (0-based index for multiple images per object)
        # For now, assuming 1 image per object
        df['img_rank'] = 0
 
 
        ## 3.2 Prepare TSV file for this sample ----

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
        with open(tsv_file, 'w') as f:
            # Write header
            f.write('\t'.join(sorted_df.columns) + '\n')
            # Write type row
            f.write('\t'.join([type_row[col] for col in sorted_df.columns]) + '\n')
            # Write data rows
            sorted_df.to_csv(f, sep='\t', index=False, header=False)
        
        logger.debug(f"Saved {sorted_df.shape[1]} fields for {sorted_df.shape[0]} objects to '{tsv_file}'")
        
        if only_tsv:
            logger.debug(f"Skipping zip creation, only TSV file requested (--only-tsv)")
            continue


        ## 3.3 Create zip file for this sample ----
        logger.info(f"Creating '{zip_file}'")

        images_dir = Path(project) / "images" / sample_id
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add the TSV file
            zf.write(tsv_file, tsv_file.name)
            
            # Add all images from the sample's images directory
            image_files = list(images_dir.glob("*.png"))
            logger.debug(f"Adding {len(image_files)} images to zip file")
            
            for image_file in image_files:
                zf.write(image_file, image_file.name)
        
        logger.debug(f"Created zip file '{zip_file}' with {len(image_files)} images")


        # Remove the TSV file after adding it to the zip
        logger.debug(f"Removing temporary TSV file '{tsv_file}'")
        tsv_file.unlink()

    log_command_success(logger, "Prepare EcoTaxa files")
