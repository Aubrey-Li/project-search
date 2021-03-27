package search.src

import scala.collection.mutable.{HashMap, HashSet}

class indexTest {
  /**
    * test cases:
    * 1. test if idToTitles HashMap generate the desired results
    * 2. test if
    */
//  // titles.txt Maps the document ids to the title for each document
//  private val idsToTitle = new HashMap[Int, String]
val testIdsToTitle = new HashMap[Int, String]

//  // words.txt Map term to id and frequency of term in that page
//  private val termsToIdFreq = new HashMap[String, HashMap[Int, Double]]
//
//  // docs.txt Map id to count of the most frequent term in that document
//  private val idsToMaxCounts = new HashMap[Int, Double]
//
//  // docs.txt Map document id to the page rank for that document
//  private val idsToPageRank = new HashMap[Int, Double]
//
//  // page ids to Set of link ids (we use HashSet to avoid duplicates in links)
//  private val idToLinkIds = new HashMap[Int, HashSet[Int]]
//
//  // page title mapping to its id
//  private val titleToIds = new HashMap[String, Int]

  // Create words in the file, manually calculate
  // idsToTitle = { 0 : title1, 1 : title2, 2 : title3 }
  // termsToIdFreq = { goodness : { 0 : 1, 2 : 1 }, lovely : { 0 : 1, 2 : 1}, drop : { 0 : 1,
  // title counts twice: once from title tag, once from [[link]]
  val testTermsToIdFreq = new HashMap[String, HashMap[Int, Double]]
  testTermsToIdFreq("goodness")(0) = 1
  testTermsToIdFreq("goodness")(2) = 1
  testTermsToIdFreq("lovely")(0) = 1
  testTermsToIdFreq("lovely")(2) = 1
  testTermsToIdFreq("drop")(0) = 1
  testTermsToIdFreq("drop")(1) = 1

  // test if generated hashmap is correct
  // use getters to fetch the hashmaps we generate

}
