from setuptools import setup

setup(
    name="StEWI",
    version="1.1.3",
    author="Ben Young, Wesley Ingwersen, Matthew Bergmann, Jose Hernandez-Betancur, Tapajyoti Ghosh, Eric Bell",
    author_email="ingwersen.wesley@epa.gov",
    description="Standardized Emission And Waste Inventories (StEWI)"
                "provides processed EPA release and emissions inventories "
                "in standard tabular format",
    license="MIT",
    keywords="USEPA data",
    url="http://www.github.com/usepa/standardizedinventories",
    packages=['chemicalmatcher', 'facilitymatcher', 'stewi', 'stewicombo'],
    include_package_data=True,
    python_requires=">=3.8",
    install_requires=[
        'esupy @ git+https://github.com/USEPA/esupy.git#egg=esupy',
        'numpy>=1.20.1',
        'pandas>=1.3',
        'requests>=2.20',
        'beautifulsoup4>=4.9.3',
        'PyYAML>=5.1',
        'openpyxl>=3.0.7',
        'xlrd>=2.0.0',
        ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: MIT",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ]
)
