package search.sol

import scala.collection.mutable.HashMap
import scala.util.matching.Regex
import scala.xml.Node

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

  // 1. parsing + tokenizing
  // for page in (rootNode \ "page")
  //    (page \\ "id")
  //    (page \\ "title")
  // -> (rootNode \ "page").text gives us concatenated string of all content of a page
  // for pageString in (rootNode \ "page").text
  //    val matchesIterator = regex.findAllMatchesIn(pageString)
  //    val matchesList = matchesIterator.toList.map{ aMatch => aMatch.matched }
  // -> using regex we get a list of all words of a page (each element is a word or link)

  /**
   * A function that parse the document and creates a HashMap with its id as key and the list of words in the corpus
   * as the value
   *
   * @return hashmap of page id to List of words in that page (with punctuation, whitespace removed)
   *         and links, pipe links, and metapages parsed to remove square brackets
   */
  def parsing(): HashMap[Int, List[String]] = {
    val rootNode: Node = xml.XML.loadFile("smallWiki.xml")
    val idToParsedWords = new HashMap[Int, List[String]]
    for (page <- rootNode \ "page") {
      val id: Int = (page \\ "id").asInstanceOf[Int]
      // get concatenation of all text in the page
      val pageString: String = page.text
      // remove punctuation and whitespace, matching all words including pipe links and meta pages
      // TODO:--> figure out how to handle links, pipe links and meta pages separately (removing square brackets)
      // TODO: remove ids?
      val matchesIterator = regex.findAllMatchIn(pageString)
      // convert to list (each element is a word of the page)
      val matchesList = matchesIterator.toList.map { aMatch => aMatch.matched }
      idToParsedWords(id) = matchesList
    }
    idToParsedWords
  }

  // handle special cases: id, pipelink, meta-page link, title
  // --> record special cases to add to hashmaps
  // --> should we remove id from our final list of terms?

  // 3. removing stop words
  // iterate over list and if isStopWord(element) == true, then remove element
  // input: list of words of a page
  // output: that same list without stop words
  //  def

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


}

class PageRank(titleToLinks: HashMap[String, Array[String]], idToTitles: HashMap[Int, String]) {

  val totalPageNum: Int = titleToLinks.size
  val allPages: Array[String] = new Array[String](idToTitles.size)
  for (value <- idToTitles.values) {
    allPages :+ value
  }

  def weightMatrix(): HashMap[String, HashMap[String, Double]] = {
    val epsilon: Double = 0.15
    var weight: Double = 0.0
    //initialize empty outer hashmap
    val outerHashMap = new HashMap[String, HashMap[String, Double]]()
    for ((j, links) <- titleToLinks) {
      val totalLinks: Int = titleToLinks(j).length
      //initiating new inner hashMap
      val innerHashMap = new HashMap[String, Double]()
      for (k <- links) {
        titleToLinks(j) = titleToLinks(j).distinct //remove duplicates (ignore multiple links from one page to another
        if (totalLinks == 0) { //if the page doesn't link to anything-->link once to everywhere except itself
          weight = epsilon / totalPageNum + (1 - epsilon) / (totalPageNum - 1)
        }
        if (titleToLinks(k).contains(j) && !j.equals(k)) {
          weight = epsilon / totalPageNum + (1 - epsilon) / totalLinks
        }
        if (j.equals(k) | !allPages.contains(k)) { //links from a page to itself | link to pages outside corpus -> ignored
          weight = 0.0
        }
        else {
          weight = epsilon / totalPageNum
        }
        innerHashMap(k) = weight
      }
      outerHashMap(j) = innerHashMap
    }
    outerHashMap
  }

  def distance(previous: Array[Double], current: Array[Double]): Double = {
    // euclidean distance = sqrt of (sum of all (differences)^2)
    var differenceSum: Double = 0.0
    for (i <- 0 until previous.length - 1) {
      differenceSum += Math.pow(previous(i) - current(i), 2)
    }
    Math.sqrt(differenceSum)
  }
}

  object Index {
    def main(args: Array[String]) {
      // TODO : Implement!
      System.out.println("Not implemented yet!")
    }
  }
