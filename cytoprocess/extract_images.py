import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.extract_images")
    logger.info("extract_images: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
