import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.create")
    logger.info("create: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
