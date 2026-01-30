import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.upload")
    logger.info("upload: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
