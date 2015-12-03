from distutils.core import setup

import asyncioplus


setup(
    name="AsyncioPlus",
    version=asyncioplus.__version__,
    description="Python asyncio addons",
    url="https://github.com/NoZip/asyncioplus",
    packages=["asyncioplus"]
)
