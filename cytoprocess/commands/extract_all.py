import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.commands.extract_all")
    logger.info("extract_all: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
