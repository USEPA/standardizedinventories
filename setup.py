from setuptools import setup

install_requires=['esupy @ git+https://github.com/USEPA/esupy#egg=esupy',
                  'numpy>=1.20.1',
                  'pandas>=1.2',
                  'requests>=2.20',
                  'beautifulsoup4>=4.9.3',
                  'regex>=2021.4.4',
                  'PyYAML>=5.1',
                  'openpyxl>=3.0.7',
                  'xlrd>=2.0.0',
                  ]

setup(
    name="StEWI",
    version="0.10.0",
    author="Wesley Ingwersen, Ben Young, Matthew Bergman, Jose Hernandez-Betancur, Tapajyoti Ghosh, Mo Li",
    author_email="ingwersen.wesley@epa.gov",
    description="Standardized Emission And Waste Inventories (StEWI)"
                "provides processed EPA release and emissions inventories "
                "in standard tabular format",
    license="CC0",
    keywords="USEPA data",
    url="http://www.github.com/usepa/standardizedinventories",
    packages=['chemicalmatcher', 'facilitymatcher', 'stewi', 'stewicombo'],
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
