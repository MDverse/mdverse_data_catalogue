"""Tests for the MDverse database module."""

import mdverse.database.database as mdverse_db


def test_create():
    """Test database creation."""
    db = mdverse_db.create(in_memory=True)
    assert db is not None
