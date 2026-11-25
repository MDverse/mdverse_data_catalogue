"""Tests for the Person Pydantic model."""

import pytest

from mdverse.models.person import Person

PERSON_FIELDS = ["first_name", "last_name", "full_name", "orcid", "affiliation"]


@pytest.mark.parametrize("field_name", PERSON_FIELDS)
def test_leading_and_trailing_spaces_are_removed(field_name: str) -> None:
    """Leading and trailing whitespace is stripped from all fields."""
    person = Person(**{field_name: "  \t Some value \n "})
    assert getattr(person, field_name) == "Some value"


@pytest.mark.parametrize("field_name", PERSON_FIELDS)
@pytest.mark.parametrize("empty_value", ["", " ", "\t\n  "])
def test_empty_strings_are_converted_to_none(field_name: str, empty_value: str) -> None:
    """Empty or whitespace-only strings are converted to None."""
    person = Person(**{field_name: empty_value})
    assert getattr(person, field_name) is None


def test_inner_spaces_are_kept() -> None:
    """Whitespace inside a value is preserved."""
    person = Person(full_name=" John W. Doe ")
    assert person.full_name == "John W. Doe"


def test_none_values_are_kept() -> None:
    """Missing values remain None."""
    person = Person()
    for field_name in PERSON_FIELDS:
        assert getattr(person, field_name) is None
