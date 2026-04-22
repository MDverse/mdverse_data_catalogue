from typing import Annotated

from fastapi import Depends
from mdverse.database.database import load
from sqlmodel import Session


# Get database connection.
def get_database_session():
    engine = load("data/database.db")
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_database_session)]
