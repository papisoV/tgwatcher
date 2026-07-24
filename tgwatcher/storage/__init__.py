"""Storage package for TGWatcher.

Public API: `from tgwatcher.storage import Storage`

The `Storage` class lives in `tgwatcher/storage/facade.py` and delegates
schema migration to `tgwatcher.storage.repositories.migration.MigrationRunner`.

Future phases (see .omc/plans/ticklish-cooking-glade.md) will add more
repositories (message_repo, signal_repo, stats_repo, chat_repo) which the
facade will compose.
"""
from tgwatcher.storage.facade import Storage
from tgwatcher.storage.repositories.migration import SCHEMA_VERSION, MigrationRunner

__all__ = ["Storage", "SCHEMA_VERSION", "MigrationRunner"]
