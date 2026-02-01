"""Utility functions for cytoprocess."""

import logging
import ijson
from pathlib import Path
from datetime import datetime
import copy
import re

logger = logging.getLogger("cytoprocess.utils")

def setup_file_logging(logger: logging.Logger, project: str):
    """
    Set up file logging in the project's logs directory.
    
    Args:
        logger: The logger instance to configure
        project: The project directory path
        
    Examples:
        >>> logger = logging.getLogger("cytoprocess.example")
        >>> setup_file_logging(logger, '/path/to/project')
    """
    # Define a custom file handler that removes newlines from log messages
    # based on https://github.com/python/cpython/issues/94503
    class CleanupFormatter:
        def emit(self, record):
            s = record.getMessage()
            # Remove newlines
            s = s.replace("\n", "")
            # Remove ANSI color codes
            s = re.sub(r'\x1b\[[0-9;]*m', '', s)
            rec = copy.copy(record)
            rec.msg = s
            super().emit(rec)
    class FileHandler(CleanupFormatter, logging.FileHandler):
        pass

    # Ensure logs directory exists
    log_dir = ensure_project_dir(project, "logs")
    
    # Create a file handler with different settings from the console handler
    log_filename = f"{datetime.now().strftime('%Y-%m-%d')}_cytoprocess.log"
    file_handler = FileHandler(log_dir / log_filename, mode='a')
    file_handler.setFormatter(logging.Formatter('%(asctime)s\t%(levelname)-7s\t%(name)-16s\t%(message)s'))
    
    # modify the logger to add the file handler
    logger.addHandler(file_handler)


def log_command_start(logger: logging.Logger, message: str, project: str):
    """
    Log the start of a command execution with fancy formatting.
    
    Args:
        logger: The logger instance to use
        message: The message to log
        project: The project directory path
        
    Examples:
        >>> logger = logging.getLogger("cytoprocess.example")
        >>> log_command_start(logger, 'convert', '/path/to/project')
    """
    start = "\x1b[1;34m" # bold blue
    reset = "\x1b[0m"
    logger.info(f"\n{start}🛠️ {message} in project '{Path(project).stem}'{reset}")


def log_command_success(logger: logging.Logger, command: str):
    """
    Log the successful completion of a command with fancy formatting.
    
    Args:
        logger: The logger instance to use
        command: The command name to log

    Examples:
        >>> logger = logging.getLogger("cytoprocess.example")
        >>> log_command_success(logger, 'convert')
    """
    start = "\x1b[0;32m" # non bold green
    reset = "\x1b[0m"
    logger.info(f"{start}✅ {command} operation completed successfully{reset}")


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
            logger.warning(f"No '{key}' key found in '{json_file.name}'")
        
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
        raise FileNotFoundError(f"Directory for {kind} files not found: '{target_dir}' ; run `" + ("convert" if kind == "json" else "create") + "` command first")   
    
    # List all files of the specified kind
    logger.debug(f"Listing .{kind} files in '{target_dir}'")
    files = list(target_dir.glob("*."+kind))
    if len(files) == 0:
        logger.warning(f"No .{kind} files found in '{target_dir}'")
        return []
    logger.debug(f"Found {len(files)} .{kind} files in '{target_dir}'")
    
    # Filter by sample if specified
    sample = getattr(ctx, "obj", {}).get("sample")
    if sample:
        files = [f for f in files if f.stem == sample]
        if len(files) == 0:
            logger.warning(f"No .{kind} files found for sample '{sample}' in '{target_dir}'")
        else:
            logger.debug(f"Found file '{files[0].name}' matching sample '{sample}'")
    
    return files
