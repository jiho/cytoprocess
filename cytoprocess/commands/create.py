import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.commands.create")
    logger.info("create: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
