#
# Command line interface for CytoProcess
#
# This consists of a main command (cytoprocess) with subcommands for each processing step.
#

import logging
import sys
import click
from pathlib import Path


def _configure_logging(debug: bool):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="[%(levelname)s] %(name)s: %(message)s",
    )
# TODO log to console and to a file in the logs/ directory of the project

@click.group()
@click.option("--debug", is_flag=True, default=False, help="Enable debug logging")
@click.option("--sample", default=None, help="Limit processing to a single sample, specified by the (quoted) name of the .cyz file")
@click.pass_context
def cli(ctx, debug, sample):
    """CytoProcess command line interface."""
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    # Normalize sample name if provided (remove path and .cyz extension if present)
    if sample:
        sample_path = Path(sample)
        sample = sample_path.stem
    ctx.obj["sample"] = sample
    
    _configure_logging(debug)
    logger = logging.getLogger("cytoprocess.cli")
    logger.debug("CLI started (debug=%s, sample=%s)", debug, sample)


@cli.command()
@click.argument("project")
@click.pass_context
def create(ctx, project):
    from cytoprocess.commands import create
    create.run(ctx, project=project)


@cli.command()
@click.pass_context
def install(ctx):
    from cytoprocess.commands import install
    install.run(ctx)


@cli.command()
@click.argument("project")
@click.option("--force", is_flag=True, default=False, help="Force conversion even if JSON files already exist")
@click.pass_context
def convert(ctx, project, force):
    from cytoprocess.commands import convert
    convert.run(ctx, project=project, force=force)


@cli.command()
@click.argument("project")
@click.pass_context
def cleanup(ctx, project):
    from cytoprocess.commands import cleanup
    cleanup.run(ctx, project=project)


@cli.command(name="extract_meta")
@click.argument("project")
@click.option("--list", "list_keys", is_flag=True, default=False, help="List all metadata items found in the JSON file(s) instead of extracting some of them")
@click.pass_context
def extract_meta(ctx, project, list_keys):
    from cytoprocess.commands import extract_meta
    extract_meta.run(ctx, project=project, list_keys=list_keys)


@cli.command(name="extract_cyto")
@click.argument("project")
@click.option("--list", "list_keys", is_flag=True, default=False, help="List all cytometric parameter paths found in the JSON file(s) instead of extracting some of them")
@click.option("--force", is_flag=True, default=False, help="Force extraction even if output files already exist")
@click.pass_context
def extract_cyto(ctx, project, list_keys, force):
    from cytoprocess.commands import extract_cyto
    extract_cyto.run(ctx, project=project, list_keys=list_keys, force=force)


@cli.command(name="summarise_pulses")
@click.option("--n-poly", default=10, help="Number of degrees in polynomial approximation")
@click.pass_context
def summarise_pulses(ctx, n_poly):
    from cytoprocess.commands import summarise_pulses
    summarise_pulses.run(ctx, n_poly=n_poly)


@cli.command(name="extract_images")
@click.argument("project", type=click.Path(exists=True))
@click.option("--force", is_flag=True, help="Overwrite existing image directories")
@click.pass_context
def extract_images(ctx, project, force):
    from cytoprocess.commands import extract_images
    extract_images.run(ctx, project, force=force)


@cli.command(name="compute_features")
@click.pass_context
def compute_features(ctx):
    from cytoprocess.commands import compute_features
    compute_features.run(ctx)


@cli.command()
@click.pass_context
def prepare(ctx):
    from cytoprocess.commands import prepare
    prepare.run(ctx)


@cli.command()
@click.pass_context
def upload(ctx):
    from cytoprocess.commands import upload
    upload.run(ctx)


@cli.command(name="all")
@click.argument("project")
@click.option("--force", is_flag=True, default=False, help="Force re-processing even if output already exists")
@click.pass_context
def _all(ctx, project, force):
    """Run all processing steps in sequence"""
    from cytoprocess.commands import (
        convert,
        extract_meta,
        extract_cyto,
        summarise_pulses,
        extract_images,
        compute_features,
        prepare,
        upload,
    )
    
    logger = logging.getLogger("cytoprocess.cli")
    logger.info(f"Running all processing steps for project: {project}")
    
    try:
        logger.info("Step 1/8: Converting .cyz files to .json")
        convert.run(ctx, project=project, force=force)
        
        logger.info("Step 2/8: Extracting metadata")
        extract_meta.run(ctx, project=project, list_keys=False)

        logger.info("Step 3/8: Extracting cytometric features")
        extract_cyto.run(ctx, project=project, list_keys=False, force=force)
        
        logger.info("Step 4/8: Summarising pulse shapes")
        summarise_pulses.run(ctx)

        logger.info("Step 5/8: Extracting images")
        extract_images.run(ctx, project=project, force=force)
        
        logger.info("Step 6/8: Computing features from images")
        compute_features.run(ctx)
        
        logger.info("Step 7/8: Preparing files for EcoTaxa")
        prepare.run(ctx)
        
        logger.info("Step 8/8: Uploading to EcoTaxa")
        upload.run(ctx)
        
        logger.info("All processing steps completed successfully")
        
    except Exception as e:
        logger.error(f"Processing failed at one of the steps: {e}")
        raise


def main(argv=None):
    cli(prog_name="cytoprocess", args=argv)


if __name__ == "__main__":
    main()
