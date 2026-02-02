import logging
import pandas as pd
import zipfile
import numpy as np
from pathlib import Path
from skimage import io as skio
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


def _add_scale_bar(input_path: Path, output_path: Path, pixel_size: float):
    """
    Add a scale bar at the bottom of the image
    
    Args:
        input_path: Path to the source PNG file
        output_path: Path to write the processed PNG file
        pixel_size: Size of one pixel in mm
    """

    # Define a custom minimal 'font' for scale bar text
    f1 = np.asarray(
        [[1,1,0,1],
         [1,0,0,1],
         [1,1,0,1],
         [1,1,0,1],
         [1,1,0,1],
         [1,1,0,1],
         [1,1,0,1],
         [1,1,1,1],
         [1,1,1,1]])
    f0 = np.asarray(
        [[1,1,0,0,1,1],
         [1,0,1,1,0,1],
         [1,0,1,1,0,1],
         [1,0,1,1,0,1],
         [1,0,1,1,0,1],
         [1,0,1,1,0,1],
         [1,1,0,0,1,1],
         [1,1,1,1,1,1],
         [1,1,1,1,1,1]])
    fu = np.asarray(
        [[1,1,1,1,1,1],
         [1,1,1,1,1,1],
         [1,1,1,1,1,1],
         [1,0,1,1,0,1],
         [1,0,1,1,0,1],
         [1,0,1,1,0,1],
         [1,0,0,0,1,1],
         [1,0,1,1,1,1],
         [1,0,1,1,1,1]])
    fm = np.asarray(
        [[1,1,1,1,1,1],
         [1,1,1,1,1,1],
         [1,1,1,1,1,1],
         [0,0,1,0,1,1],
         [0,1,0,1,0,1],
         [0,1,0,1,0,1],
         [0,1,0,1,0,1],
         [1,1,1,1,1,1],
         [1,1,1,1,1,1]])
    
    # Define scale bar sizes and corresponding text
    breaks_um = np.array([1, 10, 100])
    t1um  = np.concatenate((f1, fu, fm), axis=1)
    t10um = np.concatenate((f1, f0, fu, fm), axis=1)
    t100um = np.concatenate((f1, f0, f0, fu, fm), axis=1)
    breaks_text = [t1um, t10um, t100um]
    
    # Read the grayscale image
    img = skio.imread(input_path, as_gray=True)
    img_width_px = img.shape[1]
    
    # Define how large the scale bar is for each physical size
    breaks_px = np.round(breaks_um / pixel_size)

    # Start the scale bar at these many pixels from the bottom left corner
    pad = 5

    # Find the most appropriate scale bar size given the width of the object
    break_idx = int(np.interp(img_width_px-pad, breaks_px, range(len(breaks_px))))
    
    # Pick the actual size and text we need for this size
    bar_width_px = int(breaks_px[break_idx])
    bar_text = breaks_text[break_idx]
    text_height_px,text_width_px = bar_text.shape

    # Define the width and height of the scale bar area
    w = max(img_width_px, bar_width_px+pad, text_width_px+pad)
    h = 31
    # NB: 31px matches ZooProcess
    
    # Define the scale bar area background colour as the median of the top row of the image
    backgd_clr = np.median(img[0,:]) 

    # Pad the input image on the right if it is not wide enough
    if w > img_width_px:
        padding_width = w - img_width_px
        img = np.pad(img, ((0, 0), (0, padding_width)), constant_values=backgd_clr)
    
    # Draw a blank scale bar area
    scale = np.full((h, w), backgd_clr, dtype=img.dtype)
    # Add the scale bar (black)
    scale[h-pad-2:h-pad, pad:(bar_width_px+pad)] = 0
    # Add the text (convert [0,1] to [0,backgd_clr])
    scale[h-pad-4-text_height_px:h-pad-4, pad:(text_width_px+pad)] = (bar_text * backgd_clr).astype(img.dtype)
    
    # Combine with the image
    img = np.concatenate((img, scale), axis=0)
        
    # Write the processed image
    skio.imsave(output_path, img)


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
        message = f"Samples file not found: '{samples_file}', run `cytoprocess list {project}`."
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
            logger.warning(f"Missing metadata from the instrument, run `cytoprocess --sample '{sample_id}' extract_meta {project}`")
            at_least_one_missing = True

        cytometric_file = work_dir / f"{sample_id}_cytometric_features.parquet"
        if not cytometric_file.exists():
            logger.warning(f"Missing cytometric features, run `cytoprocess --sample '{sample_id}' extract_features {project}`")
            at_least_one_missing = True
        pulses_file = work_dir / f"{sample_id}_pulses.parquet"
        if not pulses_file.exists():
            logger.warning(f"Missing pulses summary, run `cytoprocess --sample '{sample_id}' summarise_pulses {project}`")
            at_least_one_missing = True
        image_features_file = work_dir / f"{sample_id}_image_features.parquet"
        if not image_features_file.exists():
            logger.warning(f"Missing image features, run `cytoprocess --sample '{sample_id}' compute_features {project}`")
            at_least_one_missing = True

        images_dir = Path(project) / "images" / sample_id
        if not images_dir.exists():
            logger.warning(f"Images not found, run `cytoprocess --sample '{sample_id}' extract_images {project}`")
            at_least_one_missing = True
    
    if at_least_one_missing:
        logger.error(f"Missing input for some samples. Please run the required extraction steps before preparing EcoTaxa files.")
        return

    ## 4. Detect extra samples in work/ and warn the user ----

    if not sample:
        # List all samples available in work/
        work_samples = set(instrument_meta_df['sample_id'].tolist())
        for file in work_dir.glob("*_cytometric_features.parquet"):
            sample_id = file.stem.replace("_cytometric_features", "")
            work_samples.add(sample_id)
        for file in work_dir.glob("*_pulses.parquet"):
            sample_id = file.stem.replace("_pulses", "")
            work_samples.add(sample_id)
        for file in work_dir.glob("*_image_features.parquet"):
            sample_id = file.stem.replace("_image_features", "")
            work_samples.add(sample_id)
        # detect the ones not in samples.csv
        extra_samples = work_samples - set(samples)
        if extra_samples:
            logger.warning(f"NB: Detected {len(extra_samples)} sample(s) in 'work/' not listed in 'meta/samples.csv': {sorted(extra_samples)}; you should re-run `cytoprocess list {project}`.")


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
        
        logger.info(f"Collating '{tsv_file}'")

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

        # Extract pixel size from our custom column and remove it
        # (the user will have it only if he/she explicitly extracted it)
        pixel_size = np.float32(instrument_meta.iloc[0]['__pixel_size__'])
        instrument_meta = instrument_meta.drop(columns=['__pixel_size__'])

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
        logger.info(f"Assembling '{zip_file}'")

        images_dir = Path(project) / "images" / sample_id
        image_files = list(images_dir.glob("*.png"))
        processed_images = []
        
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add the TSV file
            zf.write(tsv_file, tsv_file.name)
            
            # Process and add all images from the sample's images directory
            logger.debug(f"Processing and adding {len(image_files)} images to zip file")
            
            for image_file in image_files:
                # Process image: add scale bar at bottom
                processed_path = ecotaxa_dir / image_file.name
                _add_scale_bar(image_file, processed_path, pixel_size)
                processed_images.append(processed_path)
                
                # Add to zip
                zf.write(processed_path, processed_path.name)
        
        logger.debug(f"Created zip file '{zip_file}' with {len(image_files)} images")


        # Remove the TSV file after adding it to the zip
        logger.debug(f"Removing temporary TSV file '{tsv_file}'")
        tsv_file.unlink()
        
        # Remove processed images after adding them to the zip
        logger.debug(f"Removing {len(processed_images)} temporary processed images")
        for processed_path in processed_images:
            processed_path.unlink()

    log_command_success(logger, "Prepare EcoTaxa files")
