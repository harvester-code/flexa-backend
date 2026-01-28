"""
Core module for AI agent application logic.

This module contains the core business logic for AI agent command processing:
- Command parsing: Natural language to structured commands
- Command execution: Executing parsed commands (add/remove process, list files, read files, etc.)
"""

from .command_parser import CommandParser
from .command_executor import CommandExecutor

__all__ = [
    "CommandParser",
    "CommandExecutor",
]
