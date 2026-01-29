import logging


def run(ctx, file=None):
    logger = logging.getLogger("cytoprocess.convert")
    logger.info("convert: called with file=%s", file)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
