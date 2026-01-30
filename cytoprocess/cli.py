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


@cli.command(name="extract_list")
@click.pass_context
def extract_list(ctx):
    from cytoprocess.commands import extract_list

    extract_list.run(ctx)


@cli.command(name="extract_pulses")
@click.option("--n-poly", default=10, help="Number of degrees in polynomial approximation")
@click.pass_context
def extract_pulses(ctx, n_poly):
    from cytoprocess.commands import extract_pulses

    extract_pulses.run(ctx, n_poly=n_poly)


@cli.command(name="extract_images")
@click.pass_context
def extract_images(ctx):
    from cytoprocess.commands import extract_images

    extract_images.run(ctx)


@cli.command(name="extract_features")
@click.pass_context
def extract_features(ctx):
    from cytoprocess.commands import extract_features

    extract_features.run(ctx)


@cli.command(name="extract_all")
@click.pass_context
def extract_all(ctx):
    from cytoprocess.commands import extract_all

    extract_all.run(ctx)


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
@click.pass_context
def _all(ctx):
    from cytoprocess.commands import all as all_cmd

    all_cmd.run(ctx)


def main(argv=None):
    cli(prog_name="cytoprocess", args=argv)


if __name__ == "__main__":
    main()
