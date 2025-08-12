"""
OLIDS UAT Testing Framework

A comprehensive data validation framework for the OLIDS healthcare data warehouse,
built with Snowflake Snow CLI integration and rich terminal interface.
"""

__version__ = "0.1.0"
__author__ = "OLIDS Team"
__email__ = "olids@nhs.net"

from .core.config import Config
from .core.test_base import BaseTest, TestResult

__all__ = ["Config", "BaseTest", "TestResult"]