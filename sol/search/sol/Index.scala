package search.sol

import java.io.{FileNotFoundException, IOException}

import search.src.FileIO
import search.src.StopWords.isStopWord
import search.src.PorterStemmer.stem
import search.src.PorterStemmer.stemArray

import scala.collection.mutable
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.util.matching.Regex
import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.xml.{Elem, Node, NodeSeq}

/**
  * Provides an XML indexer, produces files for a querier
  *
  * @param inputFile - the filename of the XML wiki to be indexed
  */
class Index(val inputFile: String) {
  var pages: NodeSeq = xml.XML.loadFile(inputFile) \ "page"

  // an array of all possible page ids
  val idArray: Array[Int] = (pages \ "id").toArray
    .map(x => x.text.replace("\n\\s*", "").trim().toInt)

  // store the total number of pages & set global constant epsilon
  private var totalPageNum: Int = idArray.length
  val epsilon: Double = 0.15

  // titles.txt Maps the document ids to the title for each document
  private val idsToTitle = new HashMap[Int, String]

  // words.txt Map term to id and frequency of term in that page
  private val termsToIdFreq = new HashMap[String, HashMap[Int, Double]]

  // docs.txt Map id to count of the most frequent term in that document
  private val idsToMaxCounts = new HashMap[Int, Double]

  // docs.txt Map document id to the page rank for that document
  private val idsToPageRank = new HashMap[Int, Double]

  // page ids to Set of link ids (we use HashSet to avoid duplicates in links)
  private val idToLinkIds = new HashMap[Int, HashSet[Int]]

  // page title mapping to its id
  private val titleToIds = new HashMap[String, Int]

  // Maps ids of pages (link from) to a HashMap that maps ids of pages (link to) to the weight
  val idsToWeight: Array[HashMap[Int, Double]] = new Array[mutable.HashMap[Int, Double]](totalPageNum)

  // maps from id to the index of id in the idArray
  val idToIndex = new HashMap[Int, Int]()

  // regex to remove white space and punctuation
  private val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")
  private val regexLink = """\[\[[^\[]+?\]\]"""
  private val regexLinkParser = new Regex(regexLink)
  private val regexPipeLink = """\[\[[^\[]+?\|[^\[]+?\]\]"""

  // regexes to process links in the helper functions (removes square brackets)
  private val regexPipeLinkHelper = new Regex("""[^\[\|\]]+""")
  private val regexNormalLinkHelper = new Regex("""[^\[\]]+""")

  /**
    * A helper function that populates the termsToIdFreq hashMap while stemming input words and removing stop words
    *
    * @param term - a word from a page in the corpus
    * @param id   - the id of the page this word appears in
    */
  private def termsToIdFreqHelper(term: String, id: Int, termsToFreqThisPage: HashMap[String, Int]): Unit = {
    // if term exists in map
    if (termsToFreqThisPage.contains(term)) {
      // increment frequency
      termsToFreqThisPage(term) += 1
    } else {
      // add it to the map
      termsToFreqThisPage(term) = 1
    }
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
      termsToIdFreq(term) = HashMap(id -> 1)
    }
  }

  /**
    * helper function that populates idToLinkIds
    */
  protected def populateIdToLinkIds(): Unit = {

    for (page <- pages) {
      // extract title
      val title: String = (page \ "title").text.trim()
      // extract id
      val id: Int = (page \ "id").text.trim().toInt
      // add id & title to hashmap
      titleToIds(title) = id
      idToLinkIds(id) = new HashSet[Int]()
    }

    for (page <- pages) {
      // extract id
      val id: Int = (page \ "id").text.trim().toInt
      // find all link matches using regex
      val matchesArray: Array[String] = regexLinkParser.findAllMatchIn(page.text).toArray.map { aMatch => aMatch.matched }

      // for all words on this page
      for (term <- matchesArray) {
        // case 1: pipe link
        if (term.matches(regexPipeLink)) {
          val LinkWordStrings: List[String] = regexPipeLinkHelper.findAllMatchIn(term).toList.map { aMatch => aMatch.matched }

          // extract the link name
          val linkName = LinkWordStrings.head //e.g. "Leaders"

          // adding the id of the link to idToLinkIds
          if (titleToIds.keySet.contains(linkName)) {
            if (!idToLinkIds.keySet.contains(id)) {
              idToLinkIds(id) += titleToIds(linkName)
            } else {
              idToLinkIds += (id -> HashSet(titleToIds(linkName)))
            }
          }
        } //case 2 and 3: normal link or meta page
        else {
          // remove punctuation and whitespace, eliminate the [[ ]]
          // convert to list, there should just be one long string in the list
          val LinkWordStrings: List[String] = regexNormalLinkHelper.findAllMatchIn(term).toList.map { aMatch => aMatch.matched }

          // adding the id of the link to idToLinkIds
          if (titleToIds.keySet.contains(LinkWordStrings.head)) {
            idToLinkIds(id) += titleToIds(LinkWordStrings.head)
          }
        }
      }
    }
  }

  /**
    * helper function that takes in a pipeLink, populates the idToLinkIds hashmap and returns an array of terms to process
    * in termsToIdFreqHelper
    *
    * @param linkString -- string of a link that contains words and underlying link
    * @param id         - id of page that this linkString appears in
    * @return - an array of words to process
    */
  private def pipeLinkHelper(linkString: String, id: Int): List[String] = {
    // remove punctuation and whitespace, matching all words including pipe links and meta pages
    // convert to list (each element is a word of the page)
    val LinkWordStrings: List[String] = regexPipeLinkHelper.findAllMatchIn(linkString).toList.map { aMatch => aMatch.matched }

    // extract the link name
    //    val linkName = LinkWordStrings.head //e.g. "Leaders"
    // string after the pipe character (words to process)
    val addToWords = LinkWordStrings(1) //e.g. "US Presidents"

    // remove white space and punctuation
    // convert to list (each element is a word of the page)
    val nonLinkWords = regex.findAllMatchIn(addToWords).toList.map { aMatch => aMatch.matched }
    nonLinkWords
  }

  /**
    * helper function that takes in a meta-link/normal link (the operations for them are the same),
    * populates the idToLinkIds hashmap and returns an array of terms to process
    * in termsToIdFreqHelper
    *
    * @param linkString -- string of a link that contains words and underlying link
    * @param id         - id of page that this linkString appears in
    * @return - an array of words to process
    */
  private def normalLinkHelper(linkString: String, id: Int): List[String] = {

    // remove punctuation and whitespace, eliminate the [[ ]]
    // convert to list, there should just be one long string in the list
    val LinkWordStrings: List[String] = regexNormalLinkHelper.findAllMatchIn(linkString).toList.map { aMatch => aMatch.matched }

    // parse the long string to words
    // convert to list
    val nonLinkWords: List[String] = regex.findAllMatchIn(LinkWordStrings.head).toList.map { aMatch => aMatch.matched }
    nonLinkWords
  }


  /**
    * A function that parse the document and creates a HashMap with its id as key and the list of words in the corpus
    * as the value
    *
    * @return hashmap of page id to List of words in that page (with punctuation, whitespace removed)
    *         and links, pipe links, and metapages parsed to remove square brackets
    */
  protected def parsing(): Unit = {

    for (page <- pages) {
      // extract id
      val id: Int = (page \ "id").text.trim().toInt
      // (all steps combined in one line to save memory!)
      // 1. get concatenation of all text in the page
      // 2. concatenate title to body, excluding ids in pageString
      // 3. remove punctuation and whitespace, matching all words including pipe links and meta pages & convert to list
      val matchesArray: Array[String] = stemArray(regex.findAllMatchIn((page \ "title").text.trim()
        .concat(" " + (page \ "text").text.trim()))
        .toArray.map { aMatch => aMatch.matched })
        .filter(word => !isStopWord(word))

      // hashmap to store terms to their frequency on this page (intermediate step for termsToIdFreq)
      val termsToFreqThisPage = new HashMap[String, Int]

      // for all words on this page
      for (term <- matchesArray) {

        // if our word is a link
        if (term.matches(regexLink)) {
          // case 1: pipe link
          if (term.matches(regexPipeLink)) {

            // extract word(s) to process (omit underlying link)
            // pass word(s) to termsToIdFreq helper
            for (linkWord <- pipeLinkHelper(term, id)) {
              // populate termsToIdFreq map (to be stemmed and stopped)
              termsToIdFreqHelper(linkWord, id, termsToFreqThisPage)
            }

          } //case 2 and 3: normal link or meta page
          else {
            // extract word(s) to process (omit underlying link)
            // pass word(s) to termsToIdFreq helper
            for (linkWord <- normalLinkHelper(term, id)) {
              // populate termsToIdFreq map (to be stemmed and stopped)
              termsToIdFreqHelper(linkWord, id, termsToFreqThisPage)
            }
          }
        }
        // our word is not a link
        else {
          // populate termsToIdFreq map (to be stemmed and stopped)
          termsToIdFreqHelper(term, id, termsToFreqThisPage)
        }
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
      termsToFreqThisPage.clear()

      //      pages = pages.filterNot(_ == page)
      val removeIt = new RewriteRule {
        override def transform(n: Node): NodeSeq = n match {
          case e: Elem => NodeSeq.Empty
          case n => n
        }
      }
      new RuleTransformer(removeIt).transform(page)
    }
  }

  protected def populateIdToTitle(): Unit = {

    // populate idsToTitle, idsToPageRank hashmaps
    for (page <- pages) {
      // extract title
      val title: String = (page \ "title").text.trim()
      // extract id
      val id: Int = (page \ "id").text.trim().toInt
      // add id & title to hashmap
      idsToTitle(id) = title
    }
  }

  //populate idToPageRank
  protected def populateIdToIndex(): Unit = {
    // populate idToIndex
    for (i <- 0 until totalPageNum) {
      idToIndex.put(idArray(i), i)
    }

    // populate idsToTitle, idsToPageRank hashmaps
//    for (page <- pages) {
//      // extract id
//      val id: Int = (page \ "id").text.trim().toInt
//      // add id & title to hashmap
//      idsToPageRank(id) = 0.0
//    }
//    totalPageNum = idsToPageRank.size
  }

  // below are the implementation for calculating page rank
  /**
    * a method to calculate all the weights between any of the two pages
    */
  private def weightDistribution(): Unit = {

    // Populate idsToWeight
    for (outerId <- idArray) {
      val linkIdSet = idToLinkIds(outerId)
      val numOfLinks = linkIdSet.size
      var weight = 0d

      // Map the page id (linked to) to the weight
      val innerMap = new HashMap[Int, Double]()

      // Populate the inner map
      for (innerId <- linkIdSet) {
        weight = (epsilon / totalPageNum) + ((1 - epsilon) / numOfLinks)
        innerMap.put(innerId, weight)
      }

      idsToWeight(idToIndex(outerId)) = innerMap
    }

  }

  /**
    * a method to calculate the Euclidean distance between 2 input arrays
    *
    * @param a - the first input array
    * @param b - the second input array
    * @return the euclidean distance between a and b, a double
    */
  private def distance(a: Array[Double], b: Array[Double]): Double = {
    var differenceSum: Double = 0
    for (i <- a.indices) {
      differenceSum += (a(i) - b(i)) * (a(i) - b(i))
    }
    Math.sqrt(differenceSum)
  }

  /**
    * a method to calculate the rank of all the pages
    *
    * @return an array filled with the rank of all the pages
    */
  private def pageRank(): Unit = {
    val current: Array[Double] = Array.fill(totalPageNum)(1d / totalPageNum)
    var previous: Array[Double] = Array.fill(totalPageNum)(0d)
    while (distance(current, previous) > 0.0001) {

      previous = current.clone
      for (j <- idArray) {
        current(idToIndex(j)) = 0
        for (k <- idArray) {
          val weight =
            if (idsToWeight(idToIndex(k)).contains(j)) {
              idsToWeight(idToIndex(k))(j)
            } else {
              epsilon / totalPageNum
            }
          current(idToIndex(j)) += previous(idToIndex(k)) * weight
        }
      }
    }

    for (i <- idArray) {
      idsToPageRank.put(i, current(idToIndex(i)))
    }
  }

}

//TO RUN:
// fill in the file name with relative path, titles.txt, docs.txt, and word.txt separated by space under
// edit configuration, program argument
object Index {
  def main(args: Array[String]): Unit = {
    try {
      if (args.length == 4) {
        val indexer = new Index(args(0))
        // have a buffer-like structure, processes a bit of info at a time and clears memory after each small step
        indexer.populateIdToTitle()
        // generate titles.txt
        FileIO.printTitleFile(args(1), indexer.idsToTitle)
        indexer.idsToTitle.clear()

        //handles just words
        indexer.parsing()
        // generate words.txt
        FileIO.printWordsFile(args(3), indexer.termsToIdFreq)
        indexer.termsToIdFreq.clear()
        //populates IdToLinkIds
        indexer.populateIdToLinkIds()
        //clear the titleToIds
        indexer.titleToIds.clear()
        indexer.populateIdToIndex()
        indexer.weightDistribution()
        indexer.pageRank()
        indexer.idToLinkIds.clear()
        // generate docs.txt
        FileIO.printDocumentFile(args(2), indexer.idsToMaxCounts, indexer.idsToPageRank)
      } else {
        println("Incorrect arguments: Please use <wikiPath> <titleIndex> <documentIndex> <wordIndex>")
      }
    } catch {
      case _: FileNotFoundException => println("The file was not found")
      case _: IOException => println("Error: IO Exception")
    }
  }


}
