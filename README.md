# plombery-sqlite-backups

plombery-sqlite-backups lets you easily backup a local sqlite database to an UNC path using [Plombery](https://github.com/lucafaggianelli/plombery).

![image](https://github.com/user-attachments/assets/05038be4-f840-44b5-a5ae-0f11f3a548fc)

# Installation
## Prerequisites
### Python
plombery-sqlite-backups was tested with Python v3.10, if you don't have it installed yet, go to the [official Python website](https://www.python.org/downloads/), download it and install it.
You can also use [Pyenv](https://github.com/pyenv/pyenv). Pyenv is a wonderful tool for managing multiple Python versions.
### Poetry
It's a good practice to install dependencies specific to a project in a dedicated virtual environment for that project. We recommend using [Poetry](https://python-poetry.org/).

## Installation
- Clone or download this repository
- To install the defined dependencies for plombery-sqlite-backups, just run the install command.

```bash
poetry install
```

# Run the app
```bash
poetry run python plombery_sqlite_backups/app.py
```
Now open the page [http://localhost:8000](http://localhost:8000) in your browser and enjoy!
