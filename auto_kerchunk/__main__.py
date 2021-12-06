from .main import app

# not adding this guard causes `pytest --doctest-modules .` to fail:
# it is trying to import this file, which causes `typer`/`click` to
# raise an error because they obviously don't know `pytest` options
if __name__ == "__main__":
    app()
