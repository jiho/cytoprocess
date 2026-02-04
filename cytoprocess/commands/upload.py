import logging
import getpass
from pathlib import Path
import time
import yaml
import keyring
import requests
from cytoprocess.utils import setup_logging, log_command_start, log_command_success

# EcoTaxa API base URL
ECOTAXA_API_URL = "https://ecotaxa.obs-vlfr.fr/api"
KEYRING_SERVICE = "cytoprocess-ecotaxa"

logger = logging.getLogger("cytoprocess.upload")


def _get_stored_token():
    """Retrieve stored token from keyring."""
    try:
        return keyring.get_password(KEYRING_SERVICE, "token")
    except Exception as e:
        logger.debug("Could not retrieve token from keyring: %s", e)
        return None


def _store_token(token: str):
    """Store token in keyring."""
    try:
        keyring.set_password(KEYRING_SERVICE, "token", token)
        return True
    except Exception as e:
        logger.warning("Could not store token in keyring: %s", e)
        return False


def _clear_token():
    """Clear stored token from keyring."""
    try:
        keyring.delete_password(KEYRING_SERVICE, "token")
    except Exception:
        pass


def _validate_token(token: str) -> bool:
    """Check if the token is still valid by calling /users/me."""
    try:
        response = requests.get(
            f"{ECOTAXA_API_URL}/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        return response.status_code == 200
    except requests.RequestException:
        return False


def _login(username: str, password: str) -> str | None:
    """
    Authenticate with EcoTaxa API and return JWT token.
    
    Returns None if authentication fails.
    """
    try:
        response = requests.post(
            f"{ECOTAXA_API_URL}/login",
            json={"username": username, "password": password},
            timeout=30,
        )
        if response.status_code == 200:
            # The API returns the token as a plain string (JSON string)
            return response.json()
        else:
            logger.error("Login failed: %s", response.text)
            return None
    except requests.RequestException as e:
        logger.error("Login request failed: %s", e)
        return None


def _get_user_info(token: str) -> dict | None:
    """Get current user information."""
    try:
        response = requests.get(
            f"{ECOTAXA_API_URL}/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()
        return None
    except requests.RequestException:
        return None


def _get_project_info(token: str, project_id: int) -> dict | None:
    """
    Get project information from EcoTaxa.
    
    Args:
        token: JWT authentication token
        project_id: EcoTaxa project ID
        
    Returns:
        Project information dict or None if request fails.
        Contains fields like 'title', 'projid', 'status', etc.
    """
    try:
        response = requests.get(
            f"{ECOTAXA_API_URL}/projects/{project_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 403:
            logger.error("Access denied to project %d", project_id)
        elif response.status_code == 404:
            logger.error("Project %d not found", project_id)
        return None
    except requests.RequestException as e:
        logger.error("Failed to get project info: %s", e)
        return None


def _get_project_samples(token: str, project_id: int) -> set[str]:
    """
    Get the set of sample IDs that exist in an EcoTaxa project.
    
    Args:
        token: JWT authentication token
        project_id: EcoTaxa project ID
        
    Returns:
        Set of sample IDs (orig_id) in the project.
    """
    try:
        response = requests.get(
            f"{ECOTAXA_API_URL}/samples/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"project_ids": str(project_id), "id_pattern": "*"},
            timeout=60,
        )
        if response.status_code == 200:
            samples = response.json()
            # Extract sample orig_id from the response
            return {s.get("orig_id", "") for s in samples if s.get("orig_id")}
        else:
            logger.warning("Failed to get samples: %s", response.text)
            return set()
    except requests.RequestException as e:
        logger.warning("Failed to get project samples: %s", e)
        return set()


def authenticate(username: str | None = None, password: str | None = None) -> str | None:
    """
    Authenticate with EcoTaxa API.
    
    First tries to use a stored token. If not available or invalid,
    uses provided credentials or prompts the user.
    
    Args:
        username: Optional email address. If not provided, will prompt.
        password: Optional password. If not provided, will prompt.
        
    Returns:
        JWT token if authentication successful, None otherwise.
    """
    # Try stored token first
    token = _get_stored_token()
    if token and _validate_token(token):
        user_info = _get_user_info(token)
        if user_info:
            logger.info("Authenticated as: %s (%s)", 
                       user_info.get("name", "Unknown"),
                       user_info.get("email", "Unknown"))
        return token
    elif token:
        logger.info("Stored token is invalid, need to re-authenticate")
        _clear_token()
    
    # Use provided credentials or prompt
    if not username:
        print("\nEcoTaxa Authentication Required")
        username = input("username (email): ").strip()
    if not username:
        logger.error("EcoTaxa username is required")
        return None
    
    if not password:
        password = getpass.getpass("password: ")
    if not password:
        logger.error("EcoTaxa password is required")
        return None
    
    # Attempt login
    token = _login(username, password)
    if token is None:
        logger.error("Authentication failed. Please check your EcoTaxa username and password.")
        return None
    
    # Store the token
    if _store_token(token):
        logger.info("Authentication token stored securely in system keyring")
    
    # Show user info
    user_info = _get_user_info(token)
    if user_info:
        print(f"\nAuthenticated as: {user_info.get('email', 'Unknown')}")
    
    return token


def upload_file(token: str, zip_path: Path) -> dict:
    """
    Upload a zip file to EcoTaxa user's file area.
    
    Args:
        token: JWT authentication token
        zip_path: Path to the zip file to upload
        
    Returns:
        Dictionary with 'server_path' if successful, or 'errors' list if failed.
    """
    if not zip_path.exists():
        return {"errors": [f"File not found: {zip_path}"]}
    
    logger.info(f"Uploading '{zip_path.name}' to EcoTaxa...")
    
    try:
        # NB: upload is a synchronous operation now, not a job
        #     so we can only wait for the response
        with open(zip_path, "rb") as f:
            response = requests.post(
                f"{ECOTAXA_API_URL}/user_files/",
                headers={"Authorization": f"Bearer {token}"},
                files={"file": (zip_path.name, f, "application/zip")},
                timeout=300,  # 5 minutes for upload
                # TODO make this configurable in config.yaml and or CLI
            )
        
        if response.status_code != 200:
            return {"errors": [f"File upload failed: {response.text}"]}
        
        server_path = response.json()
        logger.info(f"File uploaded to: {server_path}")
        return {"server_path": server_path}
        
    except requests.RequestException as e:
        return {"errors": [f"File upload failed: {e}"]}


def import_file(token: str, project_id: int, server_path: str) -> dict:
    """
    Start an import job for a file already uploaded to EcoTaxa.
    
    Args:
        token: JWT authentication token
        project_id: EcoTaxa project ID
        server_path: Path to the file on EcoTaxa server (from upload_file)
        
    Returns:
        Dictionary with 'job_id' if successful, or 'errors' list if failed.
    """
    logger.info(f"Starting import to project {project_id}...")
    
    import_req = {
        "source_path": server_path,
        "skip_loaded_files": False,
        "skip_existing_objects": False,
        "update_mode": "",
    }
    
    try:
        response = requests.post(
            f"{ECOTAXA_API_URL}/file_import/{project_id}",
            headers={"Authorization": f"Bearer {token}"},
            json=import_req,
            timeout=60,
        )
        
        # let the import job start
        time.sleep(2)

        if response.status_code == 200:
            result = response.json()
            if result.get("job_id", 0) > 0:
                logger.info(f"Import job created: {result['job_id']}")
            return result
        else:
            return {"errors": [f"Import failed: {response.text}"]}
            
    except requests.RequestException as e:
        return {"errors": [f"Import request failed: {e}"]}


def get_job(token: str, job_id: int) -> dict | None:
    """
    Get job status from EcoTaxa API.
    
    Args:
        token: JWT authentication token
        job_id: Job ID to check
        
    Returns:
        Job information dict or None if request fails
    """
    try:
        response = requests.get(
            f"{ECOTAXA_API_URL}/jobs/{job_id}/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()
        return None
    except requests.RequestException:
        return None


def monitor_job(token: str, job_id: int, poll_interval: float = 2.0) -> bool:
    """
    Monitor a job until it completes.
    
    Args:
        token: JWT authentication token
        job_id: Job ID to monitor
        poll_interval: Seconds between status checks
        
    Returns:
        True if job completed successfully (state 'F'), False otherwise
    """    
    last_progress = -1
    while True:
        job_info = get_job(token, job_id)
        if job_info is None:
            logger.error("Failed to get job status")
            return False
        
        state = job_info.get("state", "")
        progress = job_info.get("progress_pct", 0) or 0
        progress_msg = job_info.get("progress_msg", "")
        
        # Only print if progress changed
        if progress != last_progress:
            print(f"  Progress: {progress}% - {progress_msg}")
            last_progress = progress
        
        # Check terminal states
        # P: Pending, R: Running, A: Asking, E: Error, F: Finished
        if state == "F":
            logger.info("Job completed successfully")
            return True
        elif state == "E":
            errors = job_info.get("errors", [])
            logger.error(f"Job failed with errors: {errors}")
            return False
        elif state == "A":
            # Job is asking for user input - we can't handle this
            logger.error("Job requires user input on EcoTaxa web interface")
            return False
        
        time.sleep(poll_interval)


def run(ctx, project, username: str | None = None, password: str | None = None):
    """
    Upload prepared EcoTaxa zip files to an EcoTaxa project.
    
    Args:
        ctx: Click context
        project: Path to the Cytoprocess project
        username: Optional EcoTaxa email address
        password: Optional EcoTaxa password
    """
    logger = setup_logging(command="upload", project=project, debug=ctx.obj["debug"])

    log_command_start(logger, "Uploading samples to EcoTaxa", project)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    project = Path(project)
    # TODO abstract cheching the existence of the prpoject and of some files in it in a function; have it raise a FileNotFound error and handle it with try:except in cli.py

    # Load config from project
    config_path =  project / "config.yaml"
    if not config_path.exists():
        logger.error("Config file not found: %s", config_path)
        return

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}
    
    # Get project_id from config
    ecotaxa_config = config.get("ecotaxa", {}) or {}
    project_id = ecotaxa_config.get("project_id")
    
    if not project_id:
        logger.error("EcoTaxa project_id is not set in config.yaml")
        logger.error("Please edit config.yaml and set 'ecotaxa: project_id' to your EcoTaxa numeric project ID")
        logger.info("You can find the project ID in the URL when viewing your project on EcoTaxa")
        return

    ecotaxa_dir = project / "ecotaxa"

    # Authenticate
    token = authenticate(username=username, password=password)
    if token is None:
        logger.error("Authentication failed, cannot proceed with upload")
        return
    
    # Find zip files to upload
    zip_files = sorted(ecotaxa_dir.glob("ecotaxa_*.zip"))
    if not zip_files:
        logger.warning("No ecotaxa_*.zip files found in %s", ecotaxa_dir)
        return
    
    logger.info(f"Found {len(zip_files)} zip file(s) to upload")
    
    # Get and display project name
    project_info = _get_project_info(token, project_id)
    if project_info:
        project_name = project_info.get("title", "Unknown")
        logger.info(f"Project name: {project_name}")
    else:
        project_name = "Unknown"
        logger.warning("Could not retrieve project information")
    logger.info(f"Uploading to EcoTaxa project '{project_name}' [{project_id}]")
    
    # Get existing samples in the project
    existing_samples = _get_project_samples(token, project_id)
    logger.info(f"Found {len(existing_samples)} existing sample(s) in project")
    
    # Process each zip file: upload, import, and monitor until complete
    for i, zip_path in enumerate(zip_files, 1):
        # Extract sample ID from filename (ecotaxa_<sample_id>.zip)
        sample_id = zip_path.stem.replace("ecotaxa_", "")
        
        # Skip if sample already exists
        if sample_id in existing_samples:
            print(f"\n[{i}/{len(zip_files)}] Skipping: {zip_path.name} (sample '{sample_id}' already exists)")
            continue

        print(f"\n[{i}/{len(zip_files)}] Processing: {zip_path.name}")
        
        # Upload
        upload_result = upload_file(token, zip_path)
        logger.debug(f"Upload result: {upload_result}")
        
        if upload_result.get("errors"):
            for error in upload_result["errors"]:
                logger.error(f"  Error: {error}")
            continue
        
        server_path = upload_result.get("server_path")
        if not server_path:
            logger.warning(f"  No server path returned, upload may have failed")
            continue
        
        print(f"  ✓ Uploaded to EcoTaxa")
        
        # Import
        server_directory = Path(server_path).stem
        import_result = import_file(token, project_id, server_directory)
        logger.debug(f"Import result: {import_result}")
        
        if import_result.get("errors"):
            for error in import_result["errors"]:
                logger.error(f"  Error: {error}")
            continue
        
        job_id = import_result.get("job_id", 0)
        if job_id <= 0:
            logger.warning("  No job ID returned, import may have failed")
            continue
        
        logger.info(f"  Import job ID: {job_id}")
        print(f"  → Import job started (ID: {job_id}), monitoring progress...")
        
        # Monitor job until completion
        success = monitor_job(token, job_id)
        if success:
            print(f"  ✓ Import completed successfully")
        else:
            print(f"  ✗ Import failed or requires manual intervention")

    log_command_success(logger, "Upload completed")
