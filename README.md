# Data engineering I: Project

## Analysis of Reddit data

Group 12: Jacob Henningsson, Stefanos Tsampanakis, Sarath Peringayi Suresh, Rabia Bashir, Jakob Nyström

## 1. Introduction

## 2. Data

The analysis will be conducted on an archive of Reddit data available from [here](http://files.pushshift.io/reddit/comments/).
The data covers Reddit comments from December 2005 until January 2023, at the time of writing.
It's stored as compressed `json` files (in `.zst` format). Each Reddit comment has the
following schema / keys:

* author: Username of the author of the comment 
* author_flair_css_class: 
* author_flair_text:
* body: The message body
* can_gild: 
* controversiality:
* created_utc:
* distinguished:
* edited: If the comment has been edited (true / false)
* gilded:
* id:
* is_submitter:
* link_id:
* parent_id:
* permalink:
* retrieved_on:
* score:
* stickied:
* subreddit: Name of the subreddit (forum)
* subreddit_id: Unique identifier of the subreddit

## 3. Project report

## 4. Developer guidelines and best practice

### Subjective Style Choices
Some subjective code style decision that should be enforced on this project:

* Use [Black](https://github.com/psf/black) to format code `black -S`
* Use Google Docstring format (see [Style Guide](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html))
* Use 88 character line lengths (enforced by flake8)

### Git

Git is used for version control. We use [GitFlow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow)
to organize our branches. This means that we have protected branches that we never
directly work on (`develop`, `main`) and that all work is done on `feature/`, `bugfix/`, or
`docs/` branches that are later merged back into a protected branch using a pull request.

The usual way of working would be to:

* Checkout `develop` branch
* Create a new branch from develop prefixed with the type, e.g. `feature/string-preprocessing`
* Work on your branch
* Merge into `develop` using a Pull Request (see section below)

### Code Reviews and Pull Requests

All code changes should be reviewed by **another** team member before being merged into `develop` or `main` branches. Tag reviewers when ready to be reviewed. Delete the feature branch after PR is merged. At least one approving review and that conversations are resolved is required to merge a PR. Anyone in the team can merge a PR if these conditions are fulfilled (either creator or reviewer). 

## Python language rules

### Linting

All code should follow the [PEP-8 -- Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/)
written by Guido van Rossum. These style rules are enforced using 
[flake8](https://github.com/pycqa/flake8) linter. To run the linter locally execute

```bash
flake8 ./pricing
```

### Imports

Imports should be explicit and not use wildcards. For example:

```python
from module import *  # Avoid
from module import MyClass, my_function  # Good
```

### Naming Conventions

* Function names follow the snake case (underscore) pattern, e.g. `my_function()` (this of course does not apply to built-in Spark methods that use camelCase).
* Class names follow the CamelCase pattern, e.g. `MyClass`.
* Constants are always uppercase, e.g. `MY_CONSTANT = 1`.
* Use f-strings `f"hello {var}"` over placeholder `"hello %(var)s"` where possible
* Prefix Spark and Pandas Dataframes with `df_`, and Spark RDDs with `rdd_`
* Function names start with verb where appropriate, e.g. `get_data()` or `count_words()`

### Variable names

Use descriptive and self-explanatory variable names that are not too long.

### Type Hints

Type hints should ideally be used to specify the variable type (like in Java or C++) in each function.

For example:

```python
def add(a, b):
    """Bad"""
    return a + b

def add(a: int, b: int) -> int:
    """Good"""
    return a + b
```

### Docstrings

Use Google Docstring Format consistently to make it easier to read the code and generate documentation (e.g. using Sphinx).

For example:

```python
def process_csv(file_path: str) -> bool:
    """Read data from file and restructure format.

    The columns and their names given in the file is
    different from how the data is used within the model.
    We must filter and rename certain columns to follow
    our common pattern.

    Args:
        file_path: The file path to the file to be read.

    Returns:
        bool: True if it successfully processed the file.

    """
```
