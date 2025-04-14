"""Transform package for the declarative beam pipeline framework.

This package contains all the transform implementations for the framework.
"""

# Import all transform modules to ensure they get registered
from declarative_beam.transforms.io import text
from declarative_beam.transforms.processing import basic as processing_basic
from declarative_beam.transforms.processing import advanced as processing_advanced
from declarative_beam.transforms.aggregation import basic as aggregation_basic
from declarative_beam.transforms.window import basic as window_basic