import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.commands.extract_list")
    logger.info("extract_list: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
