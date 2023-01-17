def pytest_addoption(parser):
    parser.addoption("--warehouse-type", action="store", default=None)
