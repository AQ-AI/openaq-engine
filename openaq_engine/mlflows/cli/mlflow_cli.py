from mlflows.utils import parametrized


@parametrized
def experiment_options(fn):

    return fn


@parametrized
def from_run_id_options(fn):
    return fn


@parametrized
def previous_run_options(fn):
    return fn
