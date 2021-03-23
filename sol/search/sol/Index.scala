package search.sol

import search.src.{PorterStemmer, StopWords}

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

  // why need this? Shouldn't we have ids to Links? For pagerank calculation? (link helpers populate it)
  private val titlesToLinks = new HashMap[String, Set[String]]
  // page ids to Set of link ids
  private val idToLinks = new HashMap[Int, Set[Int]]

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word --> querier calculates tf and idf
  //  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]

  private val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")

  //--> this will return only the content within the [[ ]] i.e. "This is a [[hammer]]" will only return "hammer"
  // [[presidents|washington]] will return "presidents|washington"
  //private val regexLinks = new Regex("""(?<=\[\[).+?(?=\])""")

  private val regexLink = """\[\[[^\[]+?\]\]"""
  private val regexMetaPage = """\[\[[^\[]+?\:[^\[]+?\]\]"""
  private val regexPipeLink = """\[\[[^\[]+?\|[^\[]+?\]\]"""

  // termsToIdFreq
  // for each term in a page, we count its frequency
  // add term + id + frequency to map
  /**
    * A helper function that populates the termsToIdFreq hashMap while stemming input words and removing stop words
    *
    * @param word                - a word from a page in the corpus
    * @param id                  - the id of the page this word appears in
    * @param termsToFreqThisPage - a HashMap of stemmed terms to their frequency on this page (input id)
    */
  private def termsToIdFreqHelper(word: String, id: Int, termsToFreqThisPage: scala.collection.mutable.HashMap[String, Int]): Unit = {
    // if not stop word, stem
    if (!StopWords.isStopWord(word)) {
      val term = PorterStemmer.stem(word) // should we make this lower case? .toLowerCase
      // if stemmed version is not a stop word
      if (!StopWords.isStopWord(term)) {
        // if term exists in map
        if (termsToFreqThisPage.contains(term)) {
          // increment frequency
          termsToFreqThisPage(term) += 1
        } else {
          // add it to the map
          termsToFreqThisPage(term) = 1
        }
      } else {} // if stop word, do nothing

      // if term already exists
      if (termsToIdFreq.contains(term)) {
        // if id of term exists (page that term appears in)
        if (termsToIdFreq(term).contains(id)) {
          // increment freq of term for that id
          termsToIdFreq(term)(id) += 1
        } else {
          // add id for this existing term to the map
          termsToIdFreq(term)(id) = 1
        }
        // if term does not exist
      } else {
        // create new Term to Id to Freq map
        termsToIdFreq(term) = scala.collection.mutable.HashMap(id -> 1)
      }
      // if stop word, do nothing
    } else {}
  }

  /**
    * A function that parse the document and creates a HashMap with its id as key and the list of words in the corpus
    * as the value
    *
    * @return hashmap of page id to List of words in that page (with punctuation, whitespace removed)
    *         and links, pipe links, and metapages parsed to remove square brackets
    */
  private def parsing(): Unit = {
    val rootNode: Node = xml.XML.loadFile("smallWiki.xml")
    for (page <- rootNode \ "page") {
      // extract id
      val id: Int = (page \\ "id").text.trim().toInt
      // extract title
      val title: String = (page \\ "title").text.trim()
      // add id & title to hashmap
      idsToTitle(id) = title

      // get concatenation of all text in the page
      val pageString: String = page.text
      // remove punctuation and whitespace, matching all words including pipe links and meta pages
      val matchesIterator = regex.findAllMatchIn(pageString)
      // convert to list (each element is a word of the page)
      val matchesList = matchesIterator.toList.map { aMatch => aMatch.matched }

      // hashmap to store terms to their frequency on this page (intermediate step for termsToIdFreq)
      val termsToFreqThisPage = new scala.collection.mutable.HashMap[String, Int]

      // for all words on this page
      for (word <- matchesList) {
        // * populate titlesToLinks

        // if our word is a link
        if (word.matches(regexLink)) {

          // pipe link
          if (word.matches(regexPipeLink)) {
            // extract word(s) to process (omit underlying link)
            val wordArray: Array[String] = pipeLinkHelper(word, id)
            // pass word(s) to termsToIdFreq helper
            for (linkWord <- wordArray) {
              // populate termsToIdFreq map (to be stemmed and stopped)
              termsToIdFreqHelper(linkWord, id, termsToFreqThisPage)
            }
          }
          // metapage link
          else if (regexMetaPage.matches(word)) {
            // extract word(s) to process (omit underlying link)
            val wordArray: Array[String] = metaLinkHelper(word, id)
            // pass word(s) to termsToIdFreq helper
            for (linkWord <- wordArray) {
              // populate termsToIdFreq map (to be stemmed and stopped)
              termsToIdFreqHelper(linkWord, id, termsToFreqThisPage)
            }
          }
          // normal link
          else {
            // extract word(s) to process (omit underlying link)
            val wordArray: Array[String] = linkHelper(word, id)
            // pass word(s) to termsToIdFreq helper
            for (linkWord <- wordArray) {
              // populate termsToIdFreq map (to be stemmed and stopped)
              termsToIdFreqHelper(linkWord, id, termsToFreqThisPage)
            }
          }
        }
        // our word is not a link
        else {
          // populate termsToIdFreq map (to be stemmed and stopped)
          termsToIdFreqHelper(word, id, termsToFreqThisPage)
        }

        // * populate idsToMaxCounts map (add this page)
        // if not empty
        if (termsToFreqThisPage.nonEmpty) {
          // get max count for this page
          idsToMaxCounts(id) = termsToFreqThisPage.valuesIterator.max
        } else {
          // empty map, so max count is 0
          idsToMaxCounts(id) = 0
        }
      }
    }
  }

  // below are the implementation for calculating page rank

  // General Steps:
  // 1. create a weight matrix that store the w(j)(k) for all pages j and their links k
  // The matrix take the form of a nested HashMap that maps title to a hashmap of link titles to its weight
  // a visual representation:
  //    A   B   C
  // A
  // B
  // C

  // store the total number of pages
  private val totalPageNum: Int = titlesToLinks.size

  // create an array that contains all page titles -->useful later for checking if a link exists or not
  private val allPages: Array[String] = new Array[String](idsToTitle.size)
  for (value <- idsToTitle.values) {
    allPages :+ value
  }

  private def weightMatrix(): HashMap[String, HashMap[String, Double]] = {
    val epsilon: Double = 0.15
    var weight: Double = 0.0
    //initialize empty outer hashmap
    val outerHashMap = new HashMap[String, HashMap[String, Double]]()
    for ((j, links) <- titlesToLinks) {
      val totalLinks: Int = titlesToLinks(j).size
      //initiating new inner hashMap
      val innerHashMap = new HashMap[String, Double]()
      for (k <- links) {
        if (totalLinks == 0) { //if the page doesn't link to anything-->link once to everywhere except itself
          weight = epsilon / totalPageNum + (1 - epsilon) / (totalPageNum - 1)
        }
        if (titlesToLinks(k).contains(j) && !j.equals(k)) {
          weight = epsilon / totalPageNum + (1 - epsilon) / totalLinks
        }
        if (j.equals(k) || !allPages.contains(k)) { //links from a page to itself | link to pages outside corpus -> ignored
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

  private val weightDistribution: HashMap[String, HashMap[String, Double]] = weightMatrix()

  //idsToLinks: HashMap[id: Int, links : Array[Int]]

  private def distance(previous: Array[Double], current: Array[Double]): Double = {
    // euclidean distance = sqrt of (sum of all (differences)^2)
    var differenceSum: Double = 0.0
    for (i <- 0 until previous.length - 1) {
      differenceSum += Math.pow(previous(i) - current(i), 2)
    }
    Math.sqrt(differenceSum)
  }

  private def pageRank(): HashMap[Int, Double] = {
    var previous: Array[Double] = Array.fill[Double](totalPageNum)(0)
    var current: Array[Double] = Array.fill[Double](totalPageNum)(1 / totalPageNum)
    while (distance(previous, current) > 0.0001) {
      previous = current
      for (j <- 0 until totalPageNum) {
        current(j) = 0.0
        for (k <- 0 until totalPageNum) {
          current(j) = current(j) + weightDistribution(idsToTitle(j))(idsToTitle(k)) * previous(k)
        }
        idsToPageRank(j) = current(j)
      }
    }
    idsToPageRank
  }

}

object Index {
  def main(args: Array[String]) {
    // TODO : Implement!
    System.out.println("Not implemented yet!")
  }
}
