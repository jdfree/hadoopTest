Map Reduce coding project.
===========

To complete this project you will need:

* A local install of Hadoop 2.6.0 available here: http://hadoop.apache.org/releases.html
* Apache Maven
* Java 1.7 (1.6 and 1.8 may work, but so far we've been using 1.7)
* git

===========

Instructions

* Fork this repository and clone it.
* Use the AverageScore template and fill in the configuration, map, and reduce methods.
* A sample data file can be found in src/main/data/data.tsv
* Desired output is a file of domains extracted from the 4th (NormURL) column with an average score (2nd column) for each domain.
* When finished, please create a pull request back to this repository.
* Feel free to add any other dependencies that you like. The pom is already setup to build a 'fat' jar to include them.

===========

Desired output
* column 1: The domain, extracted from the NormURL column of the data
* column 2: An average of the Score column for the domain.

===========

Example output: (Note: your numbers may vary)


* 2007.runescape.wikia.com    0.52361506
* 247wallst.com		    0.7509867
* 3boysandadog.com	    0.6208182
* 411mania.com		    0.61947024
* abqmugshots.com		    0.4965596
* accidentalepicurean.com	    0.648543
* acultivatednest.com	    0.6184491
* adventuretime.wikia.com	    0.6283893
* againstallgrain.com	    0.6570423

Feel Free to ask questions. We're here to help.
jpowers@sovrn.com
