from setuptools import setup

install_requires=['esupy @ git+git://github.com/USEPA/esupy@v0.1.7#egg=esupy',
                  'numpy>=1.20.1',
                  'pandas>=0.22',
                  'requests>=2.20',
                  'beautifulsoup4>=4.9.3',
                  'regex>=2021.4.4',
                  ]

setup(
    name="StEWI",
    version="0.9.9",
    author="Wesley Ingwersen, Ben Young, Matthew Bergman, Jose Hernandez-Betancur, Tapajyoti Ghosh, Mo Li",
    author_email="ingwersen.wesley@epa.gov",
    description="Standardized Emission And Waste Inventories (StEWI)"
                "provides processed EPA release and emissions inventories "
                "in standard tabular format",
    license="CC0",
    keywords="USEPA data",
    url="http://www.github.com/usepa/standardizedinventories",
    packages=['chemicalmatcher', 'facilitymatcher', 'stewi', 'stewicombo'],
    # Must include package data, specifying all subdirectories to be included
    # https://setuptools.readthedocs.io/en/latest/setuptools.html#including-data-files
    package_data={'stewi': ["data/*.*",
                            "data/DMR/*.*",
                            "data/TRI/*.*",
                            "data/NEI/*.*",
                            "data/eGRID/*.*",
                            "data/RCRA/*.*",],
                  'chemicalmatcher': ["data/*.*", "output/*.*", "config.yaml"],
                  'facilitymatcher': ["data/*.*", "config.yaml"],
                  'stewicombo': ["data/*.*"]},
    include_package_data=True,
    install_requires=install_requires,
    extras_require={"RCRAInfo": ['webdriver_manager>=3.4.2',
                                 'selenium>=3.141.0']},
    classifiers=[
        "Development Status :: Beta",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: CC0",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ]
)
