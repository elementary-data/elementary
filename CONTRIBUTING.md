# Contribution guidelines

**Note**: This document contains contribution guidelines for the Elementary CLI. If you wish to contribute
to the Elementary dbt package, please refer to the [dbt package contribution guidelines](https://github.com/elementary-data/dbt-data-reliability/blob/master/CONTRIBUTING.md).

## Getting started with development

### Setup

#### (1) Clone the repository

```
git clone https://github.com/elementary-data/elementary.git
cd elementary
```

#### (2) Create then activate a virtual environment

```
python3 -m venv venv
source venv/bin/activate
```

#### (3) Install requirements

```
pip install -r dev-requirements.txt
pip install -e .
```

You're done. Running `edr` will now run the code in your local repository.

## First time contributors

If you're looking for things to help with, browse
our [issue tracker](https://github.com/elementary-data/elementary/issues)!

In particular, look for:

- [Open to contribution issues](https://github.com/elementary-data/elementary/labels/Open%20to%20contribution%20%F0%9F%A7%A1)
- [good first issues](https://github.com/elementary-data/elementary/labels/Good%20first%20issue%20%F0%9F%A5%87)
- [documentation issues](https://github.com/elementary-data/elementary/labels/documentation)

You do not need to ask for permission to work on any of these issues.
Just fix the issue yourself and [open a pull request](#submitting-changes).

To get help fixing a specific issue, it's often best to comment on the issue
itself. You're much more likely to get help if you provide details about what
you've tried and where you've
looked. [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-1b9vogqmq-y~IRhc2396CbHNBXLsrXcA) can also
be a good place
to ask for help.

## Submitting changes

Even more excellent than a good bug report is a fix for a bug, or the
implementation of a much-needed new feature.
We'd love to have your contributions.

We use the usual GitHub pull-request flow, which may be familiar to
you if you've contributed to other projects on GitHub. For the mechanics,
view [this guide](https://help.github.com/articles/using-pull-requests/).

If your change will be a significant amount of work
to write, we highly recommend starting by opening an issue laying out
what you want to do. That lets a conversation happen early in case
other contributors disagree with what you'd like to do or have ideas
that will help you do it.

The best pull requests are focused, clearly describe what they're for
and why they're correct, and contain tests for whatever changes they
make to the code's behavior. As a bonus these are easiest for someone
to review, which helps your pull request get merged quickly!

## Running tests

For every PR we merge, we require our unit-tests to pass successfully. We also
encourage you to add new unit-tests for any new functionality you added.

The tests are located under the `tests/unit` directory, and written using the
[py-test](https://docs.pytest.org/en/stable/) framework.
In order to run them, please follow these steps:

1. Install dependencies:

```bash
# Run in the root directory of the repository
pip install -r dev-requirements.txt
```

2. Run the tests:

```bash
cd tests
py.test -vvv --target <your_target>
```

## Pre-commit hooks

We use several pre-commit hooks in order to ensure code quality. These will also
run in the CI, but we also recommend to install them locally using the following command (requires
installing the dev-requirements as shown in the previous section):

```bash
# Run in the root directory of the repository
pre-commit install
```
