"""Utility to load default model in ctparse"""

import bz2
import logging
import os
import pickle
from .scorer import Scorer, DummyScorer
from .nb_scorer import NaiveBayesScorer

logger = logging.getLogger(__name__)

# Location of the default model, included with ctparse
DEFAULT_MODEL_FILE = os.path.join(os.path.dirname(__file__), "models", "model.pbz")


def load_default_scorer() -> Scorer:
    resource = 'model.pbz'

    path = os.path.join(os.path.dirname(__file__), resource)

    # logger.warning(path)
    # debug
    # logger.warning([x.name for x in pkgutil.walk_packages()])

    # for exec usage
    if os.access(path, mode=os.F_OK):
        # d = os.path.dirname(sys.modules[package].__file__)
        # logger.warning(os.path.join(d, resource))
        with bz2.open(path, 'rb') as f:
            # logger.warning(str(f))
            mdl = pickle.load(f)
        return NaiveBayesScorer(mdl)
    # for non-exec usage
    elif os.path.exists(DEFAULT_MODEL_FILE):
        logger.info("Loading model from {} for non-exec usage".format(DEFAULT_MODEL_FILE))
        with bz2.open(DEFAULT_MODEL_FILE, "rb") as fd:
            mdl = pickle.load(fd)
        return NaiveBayesScorer(mdl)
    else:
        logger.warning("No model found, initializing empty scorer")
        return DummyScorer()
