import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.commands.extract_images")
    logger.info("extract_images: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
