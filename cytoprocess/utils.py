"""Utility functions for cytoprocess."""

import logging
import ijson
from pathlib import Path

logger = logging.getLogger("cytoprocess.utils")


def ensure_project_dir(project: str, subdir: str) -> Path:
    """
    Ensure a subdirectory exists within a project directory.
    
    Creates the directory and any parent directories if they don't exist.
    
    Args:
        project: The project directory path
        subdir: The subdirectory name (e.g., "config", "meta", "converted")
        
    Returns:
        Path object for the created/verified directory
        
    Examples:
        >>> config_dir = ensure_project_dir('/path/to/project', 'config')
        >>> meta_dir = ensure_project_dir('/path/to/project', 'meta')
    """
    target_dir = Path(project) / subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def get_json_section(json_file: Path, key: str):
    """
    Load a specific section from a JSON file using streaming.
    
    Args:
        json_file: Path to the JSON file
        key: The top-level key to extract (e.g., 'instrument', 'particles', 'images')
        
    Returns:
        The section as a dict/list, or None if not found.
        
    Examples:
        >>> instrument = get_json_section(Path('data.json'), 'instrument')
        >>> images = get_json_section(Path('data.json'), 'images')
    """
    logger.debug(f"Reading '{key}' section from {json_file.name}")

    with open(json_file, 'rb') as f:
        # Use ijson to stream only the specified part
        parser = ijson.items(f, key)
        data = next(parser, None)
        
        if data is None:
            logger.warning(f"No '{key}' key found in {json_file.name}")
        
    return data


def get_sample_files(project: str, kind: str = "json", ctx=None) -> list:
    """
    Get a list of files from a project's directory.
    
    Args:
        project: The project directory path
        kind: The type of files to retrieve. Can be "json" (from converted/) or "cyz" (from raw/)
        sample: Optional sample name to filter by. If provided, only files
                matching the sample basename will be returned.
                
    Returns:
        A list of Path objects for files found. If no files are found,
        returns an empty list.
        
    Raises:
        ValueError: If kind is not "json" or "cyz"
        
    Examples:
        >>> files = get_sample_files('/path/to/project', kind='json')
        >>> files = get_sample_files('/path/to/project', kind='cyz', sample='my_sample')
    """
    
    # Determine directory and extension based on kind
    if kind == "json":
        target_dir = Path(project) / "converted"
    elif kind == "cyz":
        target_dir = Path(project) / "raw"
    else:
        raise ValueError(f"kind must be 'json' or 'cyz', got '{kind}'")
        
    if not target_dir.exists():
        raise FileNotFoundError(f"Directory for {kind} files not found: {target_dir} ; run `" + ("convert" if kind == "json" else "create") + "` command first")   
    
    # List all files of the specified kind
    logger.debug(f"Listing .{kind} files in {target_dir}")
    files = list(target_dir.glob("*."+kind))
    if len(files) == 0:
        logger.warning(f"No .{kind} files found in {target_dir}")
        return []
    logger.debug(f"Found {len(files)} .{kind} files in {target_dir}")
    
    # Filter by sample if specified
    sample = getattr(ctx, "obj", {}).get("sample")
    if sample:
        files = [f for f in files if f.stem == sample]
        if len(files) == 0:
            logger.warning(f"No .{kind} files found for sample '{sample}' in {target_dir}")
        else:
            logger.debug(f"Found file '{files[0].name}' matching sample '{sample}'")
    
    return files
