import setuptools

with open("README.md", "r") as _file:
    long_description = _file.read()

with open("requirements.txt", "r") as _file:
    requirements = _file.readlines()[:-1]

setuptools.setup(
    name="cervmongo",
    version="0.1.60",
    author="Anthony 'antcer1213' Cervantes",
    author_email="anthony.cervantes@cerver.info",
    description="An (even) higher-level MongoDB client",
    keywords="motor pymongo mongodb database nonsql db mongo",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/antcer1213/cervmongo",
    project_urls={
        "Bug Tracker": "https://github.com/antcer1213/cervmongo/issues",
        "Documentation": "https://cerver.info/packages/cervmongo/",
        "Source Code": "https://cerver.info/packages/cervmongo/_modules/",
        "Repository": "https://github.com/antcer1213/cervmongo",
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=requirements,
    extras_require={
        "settings": ["python-dotenv"],
        "aio": ["motor"],
        "extra": ["pydantic", "marshmallow", "python-magic"],
        "all": ["python-dotenv", "motor", "pydantic", "marshmallow", "python-magic"],
    },
)
