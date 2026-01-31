import logging
import yaml
import pandas as pd
from pathlib import Path
from cytoprocess.utils import get_sample_files, ensure_project_dir, get_json_section
import ijson


def _get_parameters_structure(parameters):
    """
    Extract all available paths from a particle's parameters list.
    
    Parameters is a list of dicts, each identified by its 'description' field.
    This function generates paths in the format "description.key" for all
    available keys.
    
    Args:
        parameters: List of parameter dicts, each with a 'description' key
        
    Returns:
        List of paths like ['FWS.length', 'FWS.total', 'Sidewards Scatter.length', ...]
        
    Examples:
        >>> params = [
        ...     {'description': 'FWS', 'length': 0.98, 'total': 40621.9},
        ...     {'description': 'Sidewards Scatter', 'length': 10.77, 'total': 1276.4}
        ... ]
        >>> paths = _get_parameters_structure(params)
        >>> 'FWS.length' in paths
        True
    """
    paths = []
    
    for param_dict in parameters:
        if not isinstance(param_dict, dict):
            continue
            
        description = param_dict.get('description')
        if description is None:
            continue
            
        # Add paths for all keys except 'description' itself
        for key in param_dict.keys():
            if key != 'description':
                paths.append(f"{description}.{key}")
    
    return paths


def _get_parameter_value(parameters, path):
    """
    Retrieve a value from a particle's parameters list given a path.
    
    The path format is "description.key" where description identifies the
    parameter dict and key is the field to retrieve.
    
    Args:
        parameters: List of parameter dicts, each with a 'description' key
        path: Path string (e.g., "FWS.length" or "Sidewards Scatter.total")
        
    Returns:
        The value at the given path, or None if not found.
        
    Examples:
        >>> params = [
        ...     {'description': 'FWS', 'length': 0.98, 'total': 40621.9},
        ...     {'description': 'Sidewards Scatter', 'length': 10.77}
        ... ]
        >>> _get_parameter_value(params, "FWS.length")
        0.98
        >>> _get_parameter_value(params, "Sidewards Scatter.length")
        10.77
    """
    # Split path into description and key
    parts = path.split('.', 1)
    if len(parts) != 2:
        return None
    
    description, key = parts
    
    # Find the parameter dict with matching description
    for param_dict in parameters:
        if not isinstance(param_dict, dict):
            continue
        if param_dict.get('description') == description:
            return param_dict.get(key)
    
    return None


def run(ctx, project, list_keys=False, force=False):
    logger = logging.getLogger("cytoprocess.extract_cyto")
    logger.info(f"Extracting cytometric features in project={project}")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Get JSON files from converted directory
    json_files = get_sample_files(project, kind="json", ctx=ctx)
    if not json_files:
        return
    
    logger.info(f"Processing {len(json_files)} .json file(s)")
    
    if list_keys:
        # If the --list argument is provided, extract available parameter paths
        # from the first particle of each JSON file
        
        paths = []
        for json_file in json_files:
            logger.debug(f"Listing parameter paths from {json_file.name}")
            try:
                with open(json_file, 'rb') as f:
                    # Use ijson to navigate to the particles array and get the first item
                    parser = ijson.items(f, 'particles.item')
                    first_particle = next(parser, None)
                    
                if first_particle is None:
                       logger.warning(f"No particles found in {json_file.name}")
                       continue
                    
                parameters = first_particle.get('parameters', [])
                
                if parameters is None or len(parameters) == 0:
                    logger.warning(f"No parameters found in first particle of {json_file.name}")
                    continue
                                
                paths.extend(_get_parameters_structure(parameters))
                                
            except Exception as e:
                logger.error(f"Error reading {json_file.name}: {e}")
                raise
        
        if not paths:
            logger.error("No parameter paths found in any JSON file")
            return
        
        # Deduplicate and sort
        paths = sorted(set(paths))
        logger.info(f"Found {len(paths)} parameter paths")
        
        # Write paths to file
        meta_dir = ensure_project_dir(project, "meta")
        paths_file = meta_dir / "available_cytometry_features.txt"
        with open(paths_file, 'w') as f:
            for path in paths:
                f.write(f"{path}\n")
        
        logger.info(f"Available cytometric features written to {paths_file}. Use them in the object section of the config.yaml file to define cytometric feature extraction.")
    
    else:
        # Normal operation: extract cytometric features based on config.yaml
        
        config_file = Path(project) / "config.yaml"
        logger.info(f"Read {config_file}")
        
        if not config_file.exists():
            logger.error(f"Configuration file not found: {config_file}")
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Get the 'object' section from config
        object_config = config.get('object')
        if not object_config or not isinstance(object_config, dict):
            logger.error("No 'object' section found in config.yaml")
            raise ValueError("Configuration file must contain an 'object' section with cytometric feature mappings")
        
        logger.debug(f"Found {len(object_config)} mappings in 'object' section")
        
        # Ensure meta directory exists, to store output files
        meta_dir = ensure_project_dir(project, "meta")
        
        # Process each JSON file and write one CSV per sample
        for json_file in json_files:
            sample_id = json_file.stem
            output_file = meta_dir / f"{sample_id}_cytometric_features.csv.gz"
            
            # Skip if output file exists and force is not set
            if output_file.exists() and not force:
                logger.info(f"Skipping {json_file.name}: output file already exists (use --force to overwrite)")
                continue
            
            try:
                logger.info(f"Extracting cytometric features from {json_file.name}")
                
                # Load the particles section of the json file
                particles_data = get_json_section(json_file, 'particles')
                
                if particles_data is None or len(particles_data) == 0:
                    logger.warning(f"No particles found in {json_file.name}")
                    continue
                
                logger.debug(f"Found {len(particles_data)} particles in {json_file.name}")
                
                # Prepare data structure: list of dicts, one per particle
                rows = []
                
                # Process each particle
                for particle in particles_data:
                    particle_idx = particle.get('particleId')
                    parameters = particle.get('parameters', [])
                    
                    if not parameters:
                        logger.debug(f"No parameters for particle {particle_idx} in {json_file.name}")
                        continue
                    
                    # Create a row for this particle
                    row = {
                        'sample_id': sample_id,
                        'object_id': f"{sample_id}_{particle_idx}"
                    }
                    
                    # Extract each mapped value
                    for json_path, column_name in object_config.items():
                        # Prepend 'object_' to column name for EcoTaxa compatibility
                        full_column_name = f"object_{column_name}"
                        
                        # Get the value from the parameters
                        value = _get_parameter_value(parameters, json_path)
                        
                        if value is None:
                            logger.debug(f"Path '{json_path}' not found in particle {particle_idx} of {json_file.name}")
                        
                        row[full_column_name] = value
                    
                    rows.append(row)
                
                if not rows:
                    logger.warning(f"No particle data extracted from {json_file.name}")
                    continue
                
                # Create DataFrame and save to CSV
                df = pd.DataFrame(rows)
                df.to_csv(output_file, index=False, compression='gzip')
                
                logger.info(f"Saved {df.shape[0]} particles to {output_file}")
                
            except Exception as e:
                logger.error(f"Error processing {json_file.name}: {e}")
                raise
