import logging


def run(ctx):
    logger = logging.getLogger("cytoprocess.extract_features")
    logger.info("extract_features: called")
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
