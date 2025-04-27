"""
axo/import_manager/__init__.py
~~~~~~~~~~~~~~~~~~~~~

Utility helpers for **dynamic class injection**.

Why is this needed?
-------------------
When Axo receives a pickled *class definition* over the network, that class
may belong to a module that does **not** exist in the current Python
environment (e.g. ``"myapp.models"``).  To unpickle the instance we must
re‑create the module object and register it in :pydata:`sys.modules`.

`ImportManager` defines a minimal interface for that purpose and
:class:`DefaultImportManager` provides a straightforward implementation.

Example
-------
>>> mgr = DefaultImportManager()
>>> mgr.add_module("remote_pkg.subpkg", "Dog", Dog)
>>> from remote_pkg.subpkg import Dog  # now works!
"""

from __future__ import annotations

import sys
import types
from abc import ABC, abstractmethod
from typing import Any


class ImportManager(ABC):
    """Abstract base class for dynamic‑import helpers."""

    # ------------------------------------------------------------------ #
    # API that concrete managers must implement
    # ------------------------------------------------------------------ #
    @abstractmethod
    def add_module(self, module_name: str, class_name: str, cls: type[Any]) -> None:
        """
        Ensure ``module_name`` exists in :pydata:`sys.modules` and
        inject ``cls`` under ``class_name``.

        Parameters
        ----------
        module_name :
            Dotted‑path name of the target module (e.g. ``"pkg.subpkg"``).
        class_name :
            Attribute name under which the class will be exposed.
        cls :
            The actual class object to register.
        """
        pass


class DefaultImportManager(ImportManager):
    """
    Naïve implementation that creates a *flat* :class:`types.ModuleType`
    for the requested module if it is missing and then assigns the class
    via :pyfunc:`setattr`.

    Notes
    -----
    * If ``module_name`` contains dots (nested packages) we generate
      **intermediate** dummy modules so that
      ``import pkg; import pkg.subpkg`` succeeds afterwards.
    * No thread‑safety is provided; call from a single thread at bootstrap
      or wrap in locks if used concurrently.
    """

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def add_module(self, module_name: str, class_name: str, cls: type[Any]) -> None:
        """See base‑class docstring for semantics."""
        self._ensure_module_chain(module_name)
        setattr(sys.modules[module_name], class_name, cls)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _ensure_module_chain(module_name: str) -> None:
        """
        Guarantee that *every* segment in ``pkg.subpkg.mod`` exists
        in :pydata:`sys.modules`.
        """
        parts = module_name.split(".")
        accumulated = []

        for part in parts:
            accumulated.append(part)
            mod_path = ".".join(accumulated)
            if mod_path not in sys.modules:
                sys.modules[mod_path] = types.ModuleType(mod_path)