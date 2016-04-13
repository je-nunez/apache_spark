#!/usr/bin/env scala -deprecation -J-Xmx1g -J-XX:NewRatio=4 -Dorg.xerial.snappy.lib.name=libsnappyjava.jnilib -Dorg.xerial.snappy.tempdir=/tmp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._


object SpCluster {
  def main(cmdLineArgs: Array[String]) : Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Clustering")
    sparkConf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(sparkConf)

    println("Loading file")
    val data = sc.textFile("data/scf2013.ascii")

  }

}
