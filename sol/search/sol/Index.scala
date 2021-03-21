package search.sol

import scala.collection.mutable.HashMap
import scala.util.matching.Regex

/**
 * Provides an XML indexer, produces files for a querier
 *
 * @param inputFile - the filename of the XML wiki to be indexed
 */
class Index(val inputFile: String) {
  // titles.txt Maps the document ids to the title for each document
  private val idsToTitle = new HashMap[Int, String]

  // words.txt Map term to id and frequency of term in that page
  private val termsToIdFreq = new HashMap[String, HashMap[Int, Double]]

  // docs.txt Map id to count of the most frequent term in that document
  private val idsToMaxCounts = new HashMap[Int, Double]

  // docs.txt Map document id to the page rank for that document
  private val idsToPageRank = new HashMap[Int, Double]

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word --> querier calculates tf and idf
//  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]

  val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")
}

// 1. parsing + tokenizing
// -> (rootNode \ "page").text gives us concatenated string of all content of a page
// for pageString in (rootNode \ "page").text
//    val matchesIterator = regex.findAllMatchesIn(pageString)
//    val matchesList = matchesIterator.toList.map{ aMatch => aMatch.matched }
// -> using regex we get a list of all words of a page (each element is a word or link)

// handle special cases: id, pipelink, meta-page link, title
// --> record special cases to add to hashmaps

// 3. removing stop words
// iterate over list and if isStopWord(element) == true, then remove element

// 4. stemming
// iterate over list and call stem() on each element --> mutable list so changing it
// --> hurrah now we've got a list of terms for a page

// 5. populate hashmaps

// idsToTitle
// for each page, extract title and id. Add to map
// don't stem, don't remove stop words

// termsToIdFreq
// for each term in a page, we count its frequency
// add term + id + frequency to map

// idsToMaxCounts
// for the list of terms in a page, count unique occurrences of each term
// then add max number to hashmap along with the id of the page it arises in

// idsToPageRank
// pagerank jazz
// map id to pagerank

object Index {
  def main(args: Array[String]) {
    // TODO : Implement!
    System.out.println("Not implemented yet!")
  }
}
