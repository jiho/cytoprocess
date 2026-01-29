import logging


def run(ctx, n_poly=10):
    logger = logging.getLogger("cytoprocess.commands.extract_pulses")
    logger.info("extract_pulses: called with n_poly=%s", n_poly)
    logger.debug("Context: %s", getattr(ctx, "obj", {}))
