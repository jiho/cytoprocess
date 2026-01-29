import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.commands.install")
    logger.info("install: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
