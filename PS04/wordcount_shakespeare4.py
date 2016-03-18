#!/usr/bin/spark-submit
#
# Problem Set #4
# Implement wordcount on the shakespeare plays as a spark program that:
# a.Removes characters that are not letters, numbers or spaces from each input line.
# b.Converts the text to lowercase.
# c.Splits the text into words.
# d.Reports the 40 most common words, with the most common first.

# Note:
# You'll have better luck debugging this with ipyspark

import sys
from operator import add
from pyspark import SparkContext

def isokay(ch):
    return ch in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '

if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    infile =  's3://gu-anly502/ps04/Shakespeare.txt'

    ## 
    ## Run WordCount on Spark
    ##

    sc     = SparkContext( appName="Shakespeare Count" )
    lines  = sc.textFile( infile )

    ## YOUR CODE GOES HERE
    counts = lines.flatMap(lambda line: line.split(' ')).map(lambda word: filter(unicode.isalpha,word.lower())).map(lambda x: (x, 1)).reduceByKey(add)

    top40counts = counts.sortBy(lambda x: x[1], ascending=False).take(40)

    with open("wordcount_shakespeare4.txt","w") as fout:
        for (word, count) in top40counts:
            fout.write("{}\t{}\n".format(word,count))
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
