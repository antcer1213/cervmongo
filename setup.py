import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cervmongo",
    version="0.1.13",
    author="Anthony Cervantes",
    author_email="anthony.cervantes@cerver.info",
    description="An (even) higher-level MongoDB client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/antcer1213/cervmongo",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
