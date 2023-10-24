from distutils.command.build_ext import build_ext
from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize

# Project information
NAME = 'terasort_faas'
VERSION = '0.1.0'
DESCRIPTION = 'Simple terasort benchmark in Python (built on Lithops (https://github.com/lithops-cloud/lithops), intended for FaaS).'
# URL = 'https://github.com/yourusername/your-repo'
AUTHOR = 'German Telmo Eizaguirre'
AUTHOR_EMAIL = 'germantelmo.eizaguirre@urv.cat'

# Packages
PACKAGES = find_packages()

# Dependencies
INSTALL_REQUIRES = [
    # List your project's dependencies here
]

# Read long description from README.md
with open('README.md', 'r') as f:
    LONG_DESCRIPTION = f.read()


USE_CYTHON = True

ext = '.pyx' if USE_CYTHON else '.c'

extensions = [
    Extension(
        "terasort_faas.read_terasort_data",
        ["terasort_faas/cython/read_terasort_data"+ext]
    )
]

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    # url=URL,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    packages=PACKAGES,
    install_requires=INSTALL_REQUIRES,
    # entry_points=ENTRY_POINTS,
    ext_modules=cythonize(extensions),
    cmdclass={"build_ext": build_ext}
)