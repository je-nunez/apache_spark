# spark

[![Build Status](https://travis-ci.org/je-nunez/spark.svg?branch=master)](https://travis-ci.org/je-nunez/spark)

Some tests with clustering in Apache Spark in local mode

# WIP

This project is a *work in progress*. The implementation is *incomplete* and subject to change. The documentation can be inaccurate.

# Description

It tries to do a K-Means clustering using Apache Spark in Scala on data from the [The Center for Microeconomic Data of the Federal Reserve of New York](https://www.newyorkfed.org/microeconomics/index.html). In particular, from the Excel workbook [2014 Housing Survey](https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx), available under [https://www.newyorkfed.org/microeconomics/data.html](https://www.newyorkfed.org/microeconomics/data.html).

The clustering tries to increase different cardinalities in the set of clusters, and to compare each set according to the measure of its `Within Set Sum of Squared Errors`.

**Note**: The program tries to download the Excel workbook of the `2014 Housing Survey` directly, so it depends on the availability of the link [https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx](https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/FRBNY-SCE-Housing-Module-Public-Microdata-Complete.xlsx).

# Example Output

The output reported by this program is the set of clusters found, for example, reporting one cluster *<#i>*, for some integer *i*:

     Reporting center of KMeans cluster <#i>
       ...
       ... value of the columns of the K-means center in this cluster <#i>...
       ...
     Number for non-zero columns for center of KMeans cluster <#i>: <#number-non-zero-columns>

where the value of the columns have the column index, the column name, the value of that column in the K-means center, and the description of that column.

For example:

     Reporting center of KMeans cluster 0
     ...
     Column 117 HQH6a         value:  0.55962734 Last refinanced mortgage on primary residence
     Column 118 HQH6a2_1      value:  0.70164064 Terms changed when last refinanced: The interest rate was lowered
     Column 121 HQH6a2_4      value:  0.61233999 Terms changed when last refinanced: The term of the mortgage decreased
     Column 124 HQH6a2_7      value:  0.52907140 Terms changed when last refinanced: Changed mortgage servicer
     Column 154 HQH6a3part2_1 value: -0.02503919 Magnitude of change in monthly payment
     Column 156 HQH6a4_2      value:  0.13441488 Use of money from decreased monthly payment: Paid down other debt (e.g. on credit cards, auto loans, student loans, or medical bills)
     Column 158 HQH6a4_4      value:  0.10726969 Use of money from decreased monthly payment: Used it to make renovations or improvements to the home
     Column 159 HQH6a4_5      value:  0.30495740 Use of money from decreased monthly payment: Used it to pay for other expenses
     Column 160 HQH6a4_6      value:  0.46714597 Use of money from decreased monthly payment: Used it to purchase financial assets (e.g. stocks)
     Column 163 HQH6b_1       value:  0.72465210 Percent chance will refinance over next 12 months
     ...
     Number for non-zero columns for center of KMeans cluster 0: 175
     ... [the centers of the other clusters, cluster 1 and so on, reported below] ...

Note in the sample above that the columns indexed 119, 120, 122, 123, 125 to 153, 155, 157, 161 and 162, do not appear in the center of this cluster 0 found, because their value in this center is 0.0, so they are omitted in the report for the sake of brevity.

