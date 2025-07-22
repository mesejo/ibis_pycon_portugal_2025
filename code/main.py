from typing import Union, Annotated

from fastapi import FastAPI, Depends
from ibis import BaseBackend

import ibis


def from_env():



    return ibis.duckdb.connect()

app = FastAPI()

ConnectionDep = Annotated[BaseBackend, Depends(from_env)]

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}