"""
Specific Affinity - Entity Resolution Framework

A generalized framework for matching and clustering records based on text similarity.
"""

from .config import Config
from .main import SpecificAffinity

__version__ = "1.0.0"
__all__ = ["Config", "SpecificAffinity"]
