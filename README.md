# spark

[![Build Status](https://travis-ci.org/je-nunez/spark.svg?branch=master)](https://travis-ci.org/je-nunez/spark)

Some tests with clustering in Apache Spark in local mode

# WIP

This project is a *work in progress*. The implementation is *incomplete* and subject to change. The documentation can be inaccurate.

# Description

It tries to do a K-Means clustering using Apache Spark in Scala on data from the [The Center for Microeconomic Data of the Federal Reserve of New York](https://www.newyorkfed.org/microeconomics/index.html). In particular, from the Excel workbook [2014 Housing Survey](https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx), available under [https://www.newyorkfed.org/microeconomics/data.html](https://www.newyorkfed.org/microeconomics/data.html).

The clustering tries to increase different cardinalities in the set of clusters, and to compare each set according to the measure of its `Within Set Sum of Squared Errors`.

**Note**: The program tries to download the Excel workbook of the `2014 Housing Survey` directly, so it depends on the availability of the link [https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx](https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx).

