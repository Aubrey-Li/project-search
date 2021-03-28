package search.sol

import java.io.{FileNotFoundException, IOException}

import search.src.{FileIO, PorterStemmer, StopWords}

import scala.collection.immutable.List
import scala.collection.mutable
import scala.util.matching.Regex
import scala.xml.NodeSeq

class Indexer(fileName: String) {
  //-------------------------------------------------------------------------------------------------------
  // all the fields
  //-------------------------------------------------------------------------------------------------------

  // extract the Nodes from the xml file
  val pageSeq: NodeSeq = xml.XML.loadFile(fileName) \ "page"

  // Maps the document id to its title
  val idTitleMap = new mutable.HashMap[Int, String]()

  // Maps the document title to its id
  val titleIdMap = new mutable.HashMap[String, Int]()

  // an array of all possible page ids
  val idArray: Array[Int] = (pageSeq \ "id").toArray
    .map(x => x.text.replace("\n\\s*", "").trim().toInt)

  // maps from id to the index of id in the idArray
  val idToIndex = new mutable.HashMap[Int, Int]()

  // the total number of pages
  val n: Int = idArray.length

  // Maps the document ids to the maximum word frequencies
  val idsToMaxCounts = new mutable.HashMap[Int, Double]()

  // Maps words to HashMaps that map document ids to the word frequencies
  val wordsToDocumentFrequencies = new mutable.HashMap[String, mutable.HashMap[Int, Double]]()

  // Maps the page ids to a list of all page ids the page links to
  val idsToLinkIds = new mutable.HashMap[Int, Array[Int]]()

  // Maps ids of pages (link from) to a HashMap that maps ids of pages (link to) to the weight
  val idsToWeight: Array[mutable.HashMap[Int, Double]] = new Array[mutable.HashMap[Int, Double]](n)

  // Maps ids to its page rank
  val idsToRank = new mutable.HashMap[Int, Double]()


  //-------------------------------------------------------------------------------------------------------
  // the input processor
  //-------------------------------------------------------------------------------------------------------

  /**
    * a method to covert a link to all words it contain that should be part of search
    *
    * @param link - the input link, a string
    * @return a list of string containing all the words in thel link that should be a part of search
    *         1. a regular link should just be stripped of its brackets
    *         2. a link with a : should return all the words it contain after being stemmed and
    *         stripped of stop words
    *         3. a link with a | should return all the words after the |
    */
  private def linkToWords(link: String): List[String] = {

    val stemmedLink = link.replace("[[", "")
      .replace("]]", "").replace("\n\\s*", "")
    val wordRegex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")

    //-----------------------------------------------------------------------
    if (stemmedLink.contains(":")) {
      // split the stemmedLink up by the :
      val wordColon = stemmedLink.replace(":", " ")
      val matchesColonIterator = wordRegex.findAllMatchIn(wordColon)
      val wordList = matchesColonIterator.toList.map(_.matched)
        .filter(!StopWords.isStopWord(_))
        .map(PorterStemmer.stemOneWord(_, new PorterStemmer()))
      wordList

      //-----------------------------------------------------------------------
    } else {
      if (stemmedLink.contains("|")) {

        // check if pipe is at the very beginning -> take all the words
        if (stemmedLink.substring(0).equals("|")) {
          val wordPipe = stemmedLink.replace("|", "")
          val matchesColonIterator = wordRegex.findAllMatchIn(wordPipe)
          val wordList = matchesColonIterator.toList.map(_.matched)
            .filter(!StopWords.isStopWord(_))
            .map(PorterStemmer.stemOneWord(_, new PorterStemmer()))
          wordList

          // check if pipe is at the end -> take no words
        } else {
          if (stemmedLink.substring(stemmedLink.length - 1).equals("|")) {
            List()

            // if the pip is in the middle and there are words on both sides -> take the latter part
          } else {
            // split the stemmedLink up by |
            val wordArrayLine = stemmedLink.split("\\|").map(_.trim)
            // process the words after the |
            val pageName = wordArrayLine(1)
            val matchesNameIterator = wordRegex.findAllMatchIn(pageName)
            val nameList = matchesNameIterator.toList.map(_.matched)
              .filter(!StopWords.isStopWord(_))
              .map(PorterStemmer.stemOneWord(_, new PorterStemmer()))
            // return the name list
            nameList
          }
        }

        //-----------------------------------------------------------------------
      } else {
        val matchesIterator = wordRegex.findAllMatchIn(stemmedLink)
        val wordList = matchesIterator.toList.map(_.matched)
          .filter(!StopWords.isStopWord(_))
          .map(PorterStemmer.stemOneWord(_, new PorterStemmer()))
        wordList
      }
    }
  }

  /**
    * a method to turn a link to the page title of the link
    *
    * @param rawLink - the input link, a string
    * @return the title of link, a string
    *         for example:
    *         Hammer -> Hammer
    *         Presidents|Washington -> Presidents
    *         Category:Computer Science -> Category:Computer Science
    */
  private def linkToTitle(rawLink: String): String = {
    val stemmedLink = rawLink.replace("[[", "")
      .replace("]]", "").replace("\n\\s*", "")

    if (stemmedLink.contains("|")) {
      val wordArrayLine = stemmedLink.split("\\|")
      wordArrayLine(0).trim()
    } else {
      stemmedLink
    }
  }

  /**
    * A method that converts a list of links to the page ids they link to.
    * Links that link to a document that's not in the corpus are ignores;
    * duplicated links are ignored;
    * links that link to itself are ignored;
    * when there's no link after going through previous steps, output a list of
    * all ids in the corpus except the id that the link links from
    *
    * @param rawLinks - input links
    * @param id       - the page id that all input links link from
    * @return a list of ids that links link to
    */
  private def linksToIds(rawLinks: Array[String], id: Int): Array[Int] = {

    // Filters out links that are not in the corpus
    val processedLinks = rawLinks.map(linkToTitle).filter(titleIdMap.get(_).isDefined).
      map(titleIdMap(_)).filter(_ != id).distinct

    if (processedLinks.isEmpty) {
      idArray.filter(_ != id)
    } else {
      processedLinks
    }

  }

  /**
    * the main method of the InputProcessor
    * sets up 4 hash maps: idTitleMap, idContentMap, idLinkMap, idWordMap
    */
  private def processor(): Unit = {

    // populate idToIndex
    for (i <- 0 until n) {
      idToIndex.put(idArray(i), i)
    }

    // populate idTitleMap and titleIdMap
    for (page <- pageSeq) {

      // get the id, title of each page
      val id = (page \ "id").text.replace("\n\\s*", "").trim().toInt
      val title = (page \ "title").text.replace("\n\\s*", "").trim()

      // populate up idTitleMap and titleIdMap
      idTitleMap.put(id, title)
      titleIdMap.put(title.toLowerCase(), id)

    }

    // this needs to loop again because linksToIds method needs a complete titleIdMap to work
    for (page <- pageSeq) {

      // get the id, body of each page
      val id = (page \ "id").text.replace("\n\\s*", "").trim().toInt
      val body = (page \ "text").text

      // populate idsToLinkIds
      val regex = new Regex("""\[\[[^\[]+?\]\]""")
      val matchesIterator = regex.findAllMatchIn(body)
      val linkArray =
        matchesIterator.toArray.map(_.matched.toLowerCase().replace("\n\\s*", ""))

      idsToLinkIds.put(id, linksToIds(linkArray, id))

    }

  }

  //-------------------------------------------------------------------------------------------------------
  // the relevance (TF)
  //-------------------------------------------------------------------------------------------------------

  /**
    * Calculates the word frequency of every word in one page
    *
    * @param id of the given page
    * @return a HashMap that records the word frequency of every word in this page
    */
  private def wordFrequencyOfOnePage(id: Int): mutable.HashMap[String, Double] = {

    // find the content of one page: a list of all words in the page, can have duplicates, links broken into words
    val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")
    val matchesIterator = regex.findAllMatchIn((pageSeq(idToIndex(id)) \ "text").text)
    val titleIterator = regex.findAllMatchIn((pageSeq(idToIndex(id)) \ "title").text)
    val titleWordList = titleIterator.toList.map(_.matched)
      .map(_.toLowerCase())
      .filter(!StopWords.isStopWord(_))
      .map(PorterStemmer.stemOneWord(_, new PorterStemmer()))
    val contentList = matchesIterator.toList.map(_.matched)
      .map(_.toLowerCase())
      .filter(!StopWords.isStopWord(_))
      .map(PorterStemmer.stemOneWord(_, new PorterStemmer()))
    var content = List[String]()

    // adding the words in the title
    for (word <- titleWordList) {
      content ::= word
    }

    // adding the words in the body
    for (word <- contentList) {
      // check if a word is a link
      if (word.slice(0, 2).equals("[[")) {
        // extract words in the link and adding to the wordList
        val listOfWord = linkToWords(word)
        for (w <- listOfWord) {
          content ::= w
        }
      } else {
        content ::= word
      }
    }


    // Map the word to its frequency in this document
    val wordToFrequency = new mutable.HashMap[String, Double]()

    // Initialize the map
    for (word <- content.distinct) {
      wordToFrequency.put(word, 0)
    }

    // Count each word in a page
    for (term <- content) {
      if (wordToFrequency.contains(term)) {
        val count = wordToFrequency(term)
        wordToFrequency.update(term, count + 1)
      }
    }

    wordToFrequency
  }


  /**
    * Calculates the frequency of every word in all documents and stores in wordsToDocumentFrequencies;
    * finds the max frequencies of every page and stores in idsToMaxCounts
    */
  private def wordsFrequencies(): Unit = {

    // Iterates through every id
    for (id <- idArray) {
      val wordFrequency = wordFrequencyOfOnePage(id)

      // Finds the max count of word frequency in the given page
      val maxCount: Double =
        if (wordFrequency.valuesIterator.nonEmpty) {
          wordFrequency.valuesIterator.max
        } else {
          0d
        }

      // Populate idsToMaxCounts map
      idsToMaxCounts.put(id, maxCount)

      // Populate idToWordsFrequencies map
      for (map <- wordFrequency) {
        if (wordsToDocumentFrequencies.get(map._1).isDefined) {
          wordsToDocumentFrequencies(map._1).put(id, map._2)
        } else {
          wordsToDocumentFrequencies.put(map._1, new mutable.HashMap[Int, Double]())
          wordsToDocumentFrequencies(map._1).put(id, map._2)
        }
      }

    }

  }


  //-------------------------------------------------------------------------------------------------------
  // the page rank
  //-------------------------------------------------------------------------------------------------------

  val delta = 0.15
  val gamma = 0.001

  /**
    * a method to calculate all the weights between any of the two pages
    */
  private def weightCalculation(): Unit = {

    // Populate idsToWeight
    for (outerId <- idArray) {
      val linkIdList = idsToLinkIds(outerId)
      val numOfLinksTo = linkIdList.length
      var weight = 0d

      // Map the page id (linked to) to the weight
      val innerMap = new mutable.HashMap[Int, Double]()

      // Populate the inner map
      for (innerId <- linkIdList) {
        weight = (delta / n) + ((1 - delta) / numOfLinksTo)
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
  private def euclideanDistance(a: Array[Double], b: Array[Double]): Double = {
    var itemSquareSum: Double = 0
    for (i <- a.indices) {
      itemSquareSum += (a(i) - b(i)) * (a(i) - b(i))
    }
    Math.sqrt(itemSquareSum)
  }

  /**
    * a method to calculate the rank of all the pages
    *
    * @return an array filled with the rank of all the pages
    */
  private def rankCalculation(): Unit = {
    val currentRank: Array[Double] = Array.fill(n)(1d / n)
    var copyRank: Array[Double] = Array.fill(n)(0d)
    while (euclideanDistance(currentRank, copyRank) > gamma) {

      copyRank = currentRank.clone
      for (j <- idArray) {
        currentRank(idToIndex(j)) = 0
        for (k <- idArray) {
          val weight =
            if (idsToWeight(idToIndex(k)).get(j).isDefined) {
              idsToWeight(idToIndex(k))(j)
            } else {
              delta / n
            }
          currentRank(idToIndex(j)) += copyRank(idToIndex(k)) * weight
        }
      }
    }

    for (i <- idArray) {
      idsToRank.put(i, currentRank(idToIndex(i)))
    }
  }
}

object Indexer {
  //-------------------------------------------------------------------------------------------------------
  // writes out the 3 files: title, word, doc
  //-------------------------------------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    try {
      if (args.length == 4) {
        val wikiPath = args(0)
        val titlePath = args(1)
        val docPath = args(2)
        val wordPath = args(3)
        val newIndexer = new Indexer(wikiPath)
        newIndexer.processor()
        newIndexer.wordsFrequencies()
        newIndexer.weightCalculation()
        newIndexer.rankCalculation()
        FileIO.printTitleFile(titlePath, newIndexer.idTitleMap)
        FileIO.printWordsFile(wordPath, newIndexer.wordsToDocumentFrequencies)
        FileIO.printDocumentFile(docPath, newIndexer.idsToMaxCounts, newIndexer.idsToRank)

        // handling incorrect input
      } else {
        println("Incorrect arguments: Please use <wikiPath> <titleIndex> <documentIndex> <wordIndex>")
      }
    } catch {
      case _: FileNotFoundException => println("The file was not found")
      case _: IOException => println("Error: IO Exception")
    }
  }

}