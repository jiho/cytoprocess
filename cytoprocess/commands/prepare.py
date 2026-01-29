import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.commands.prepare")
    logger.info("prepare: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
