import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.all")
    logger.info("all: called (placeholder to run all commands)")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
