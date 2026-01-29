import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.commands.extract_meta")
    logger.info("extract_meta: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
