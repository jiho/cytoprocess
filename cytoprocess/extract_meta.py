import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.extract_meta")
    logger.info("extract_meta: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
