import logging
import ijson
from pathlib import Path
from cytoprocess.utils import get_sample_files


def _get_json_structure(json_data, prefix=""):
    """
    Recursively extract all keys from a JSON object and return them as full paths.
    
    Args:
        json_data: Parsed JSON data (dict, list, or primitive)
        prefix: The current path prefix (used for recursion)
        
    Returns:
        List of full paths to all keys in the JSON structure.
        Paths are separated by dots (e.g., "data.user.name")
        List items are indicated with [] notation (e.g., "items[].name")
        
    Examples:
        >>> data = {"user": {"name": "John", "age": 30}, "active": True}
        >>> paths = _get_json_structure(data)
        >>> paths
        ['user', 'user.name', 'user.age', 'active']
        
        >>> data = {"items": [{"id": 1, "name": "Item1"}, {"id": 2}], "count": 2}
        >>> paths = _get_json_structure(data)
        >>> paths
        ['items[]', 'items[].id', 'items[].name', 'count']
    """
    paths = []
    
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            # Build the full path
            current_path = f"{prefix}.{key}" if prefix else key
            
            # Recursively get paths from nested structures
            if isinstance(value, dict):
                # For dicts, add the key and recurse
                paths.append(current_path)
                paths.extend(_get_json_structure(value, current_path))
            elif isinstance(value, list) and value:
                # For lists, add the key[] notation
                list_path = f"{current_path}[]"
                paths.append(list_path)
                # If it's a list of dicts, extract structure from first item
                # (this assummes that all items have the same structure)
                if isinstance(value[0], dict):
                    paths.extend(_get_json_structure(value[0], list_path))
            else:
                # For non-dict, non-list values, just add the key
                paths.append(current_path)
    
    return paths


def run(ctx, project, list_keys=False):
    logger = logging.getLogger("cytoprocess.extract_meta")
    logger.info(f"Extracting metadata structure from JSON files in project={project}")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
        
    # Get JSON files from converted directory
    json_files = get_sample_files(project, kind="json", ctx=ctx)
        
    logger.info(f"Processing {len(json_files)} JSON file(s)")
        
    if list_keys:
        # If the --list argument is provided, only extract structure keys from each JSON file
        keys = []
        for json_file in json_files:
            try:
                logger.debug(f"Reading 'instrument' key from {json_file.name}")
                with open(json_file, 'rb') as f:
                    # Use ijson to stream only the 'instrument' part
                    parser = ijson.items(f, 'instrument')
                    instrument_data = next(parser, None)
                    
                    if instrument_data is None:
                        logger.warning(f"No 'instrument' key found in {json_file.name}")
                        continue
                    
                    # Extract structure keys
                    keys.extend(_get_json_structure(instrument_data))
                
            except ijson.JSONError as e:
                logger.error(f"Failed to parse JSON file {json_file.name}: {e}")
                raise
            except Exception as e:
                logger.error(f"Error reading {json_file.name}: {e}")
                raise

            logger.info(f"Found {len(keys)} metadata items in '{json_file.name}'")

        # If multiple files, deduplicate keys
        if len(json_files) > 1:
            keys = list(set(keys))
            logger.info(f"Found {len(keys)} unique metadata items across all JSON files")

        # Make sure config directory exists
        config_dir = Path(project) / "config"
        config_dir.mkdir(parents=True, exist_ok=True)

        # Write keys to file
        keys_file = config_dir / "available_metadata_keys.txt"
        with open(keys_file, 'w') as f:
            for key_path in sorted(keys):
                f.write(f"{key_path}\n")
        
        logger.info(f"Keys written to {keys_file}")
    else:
        logger.info(f"Not implemented yet: extraction of specific metadata items")

