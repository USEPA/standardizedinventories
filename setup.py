from setuptools import setup

setup(
    name="StandardizedReleaseAndWasteInventories",
    version="0.1",
    author="Wesley Ingwersen",
    author_email="ingwersen.wesley@epa.gov",
    description="Standardized Emission And Waste Inventories (StEWI) provides processed EPA release and emissions inventories "
                "in standard tabular format",
    license="CC0",
    keywords="USEPA data",
    url="http://www.github.com/usepa/standardizedinventories",
    packages=['stewi'],
    package_data={'stewi': ["data/*.*","output/*.*"]},
    install_requires=['numpy', 'pandas', 'requests'],
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: GPL 3.0",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ]
)
