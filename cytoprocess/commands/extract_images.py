import logging
import base64
from pathlib import Path
from cytoprocess.utils import get_sample_files, ensure_project_dir, get_json_section


def run(ctx, project):
    logger = logging.getLogger("cytoprocess.extract_images")
    logger.info(f"Extracting images from JSON files in project={project}")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
    
    # Get JSON files from converted directory
    json_files = get_sample_files(project, kind="json", ctx=ctx)
    
    logger.info(f"Processing {len(json_files)} JSON file(s)")
    
    # Process each JSON file
    total_images = 0
    for json_file in json_files:
        try:
            logger.debug(f"Extracting images from {json_file.name}")
            
            # Create subdirectory for this sample's images
            sample_name = json_file.stem
            sample_images_dir = ensure_project_dir(project, f"images/{sample_name}")
            
            # Load the images section from the JSON file
            images = get_json_section(json_file, 'images')
            
            if images is None:
                logger.warning(f"No images found in {json_file.name}")
                continue
            
            image_count = 0
            for image in images:
                    # Extract particleId and base64 data
                    particle_id = image.get('particleId')
                    base64_data = image.get('base64')
                    
                    if particle_id is None:
                        logger.warning(f"Image item missing 'particleId' in {json_file.name}")
                        continue
                    
                    if base64_data is None:
                        logger.warning(f"Image item {particle_id} missing 'base64' data in {json_file.name}")
                        continue
                    
                    # Decode base64 data
                    try:
                        image_data = base64.b64decode(base64_data)
                    except Exception as e:
                        logger.error(f"Failed to decode base64 for particle {particle_id} in {json_file.name}: {e}")
                        continue
                    
                    # Write to PNG file
                    output_file = sample_images_dir / f"{particle_id}.png"
                    with open(output_file, 'wb') as img_file:
                        img_file.write(image_data)
                    
                    image_count += 1
                    
            logger.info(f"Extracted {image_count} images from {json_file.name}")
            total_images += image_count
                
        except Exception as e:
            logger.error(f"Error processing {json_file.name}: {e}")
            raise
    
    logger.info(f"Total images extracted: {total_images}")
