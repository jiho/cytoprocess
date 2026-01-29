import logging
import sys
import click


def _configure_logging(debug: bool):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="[%(levelname)s] %(name)s: %(message)s",
    )


@click.group()
@click.option("--debug", is_flag=True, default=False, help="Enable debug logging")
@click.option("--sample", default=None, help="Sample name")
@click.pass_context
def cli(ctx, debug, sample):
    """CytoProcess command line interface."""
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    ctx.obj["sample"] = sample
    _configure_logging(debug)
    logger = logging.getLogger("cytoprocess.cli")
    logger.debug("CLI started (debug=%s, sample=%s)", debug, sample)


@cli.command()
@click.pass_context
def create(ctx):
    from cytoprocess.commands.create import run

    run(ctx)


@cli.command()
@click.pass_context
def install(ctx):
    from cytoprocess.commands.install import run

    run(ctx)


@cli.command()
@click.option("--file", "file_path", required=True, help="Path to input cyz file")
@click.pass_context
def convert(ctx, file_path):
    from cytoprocess.commands.convert import run

    run(ctx, file=file_path)


@cli.command(name="extract_meta")
@click.pass_context
def extract_meta(ctx):
    from cytoprocess.commands.extract_meta import run

    run(ctx)


@cli.command(name="extract_list")
@click.pass_context
def extract_list(ctx):
    from cytoprocess.commands.extract_list import run

    run(ctx)


@cli.command(name="extract_pulses")
@click.option("--n-poly", default=10, help="Number of degrees in polynomial approximation")
@click.pass_context
def extract_pulses(ctx, n_poly):
    from cytoprocess.commands.extract_pulses import run

    run(ctx, n_poly=n_poly)


@cli.command(name="extract_images")
@click.pass_context
def extract_images(ctx):
    from cytoprocess.commands.extract_images import run

    run(ctx)


@cli.command(name="extract_features")
@click.pass_context
def extract_features(ctx):
    from cytoprocess.commands.extract_features import run

    run(ctx)


@cli.command(name="extract_all")
@click.pass_context
def extract_all(ctx):
    from cytoprocess.commands.extract_all import run

    run(ctx)


@cli.command()
@click.pass_context
def prepare(ctx):
    from cytoprocess.commands.prepare import run

    run(ctx)


@cli.command()
@click.pass_context
def upload(ctx):
    from cytoprocess.commands.upload import run

    run(ctx)


@cli.command(name="all")
@click.pass_context
def _all(ctx):
    from cytoprocess.commands.all import run

    run(ctx)


def main(argv=None):
    cli(prog_name="cytoprocess", args=argv)


if __name__ == "__main__":
    main()
