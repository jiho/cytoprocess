import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.extract_list")
    logger.info("extract_list: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
