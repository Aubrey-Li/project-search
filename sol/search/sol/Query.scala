package search.sol

import java.io._
import search.src.{FileIO, PorterStemmer}
import search.src.StopWords.isStopWord

import scala.collection.mutable.HashMap
import scala.util.matching.Regex

/**
 * Represents a query REPL built off of a specified index
 *
 * @param titleIndex    - the filename of the title index
 * @param documentIndex - the filename of the document index
 * @param wordIndex     - the filename of the word index
 * @param usePageRank   - true if page rank is to be incorporated into scoring
 */
class Query(titleIndex: String, documentIndex: String, wordIndex: String,
            usePageRank: Boolean) {

  // Maps the document ids to the title for each document
  private val idsToTitle = new HashMap[Int, String]

  // Maps the document ids to the euclidean normalization for each document
  private val idsToMaxFreqs = new HashMap[Int, Double]

  // Maps the document ids to the page rank for each document
  private val idsToPageRank = new HashMap[Int, Double]

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]

  // regex to remove white space and punctuation
  private val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")

  // Hashmap to store terms to inverse page frequency
  private val termToInverseFreqs = new HashMap[String, Double]

  /**
   * Helper function that adds to value in HashMap if key already exists, or inserts new k-v pair if doesn't exist
   * @param map -- Hashmap to update
   * @param key -- key
   * @param keyVal -- key value pair
   * @param f -- function to
   * @tparam K
   * @tparam V
   */
//  def updateMap[K, V](map: HashMap, key : K, keyVal: (K, V), f: V => V): Unit = {
//    map.get(key) match {
//      case Some(e) => map.update(key, f(e))
//      case None => map += keyVal
//    }
//  }

  /**
   * Handles a single query and prints out results
   *
   * @param userQuery - the query text
   */
  private def query(userQuery: String) {
    // hashmap of ids to relevancy scores for our query
    val idsToRelevancy = new HashMap[Int, Double]

    // remove punctuation and whitespace, matching all words
    val matchesIteratorAll = regex.findAllMatchIn(userQuery)

    // convert to list (each element is a word of the query)
    val queryWordList = matchesIteratorAll.toList.map { aMatch => aMatch.matched }

    for (word <- queryWordList) {
      // if not stop word
      if (!isStopWord(word)) {
        // stem, yielding a term
        val term = PorterStemmer.stem(word).toLowerCase()
        // if term is not a stop word
        if (!isStopWord(term)) {
          // if the hashmap {terms to {ids to frequencies}} contains this term
          if (wordsToDocumentFrequencies.contains(term)) {
            // for every page id mapped to this term
            for (page <- wordsToDocumentFrequencies(term).keysIterator) {
              // calculate term frequency
              // = number of times term appears in page / max frequency for this page
              val tf: Double = wordsToDocumentFrequencies(term)(page) / idsToMaxFreqs(page)
              // calculate inverse document frequency
              // log( total number of pages / number of pages that contain this term )
              val idf: Double = {
                if (termToInverseFreqs.contains(term)) {
                  termToInverseFreqs(term)
                } else {
                  val idf_intermediate = Math.log(idsToTitle.size.toDouble / wordsToDocumentFrequencies(term).keys.size)
                  termToInverseFreqs(term) = idf_intermediate
                  idf_intermediate
                }
              }

//              val idf2: Double = {
//                termToInverseFreqs.get(term) match {
//                  case None => termToInverseFreqs.put(term, Math.log(idsToTitle.size.toDouble / wordsToDocumentFrequencies(term).keys.size))
//                  case Some(v) => v.apply(value => value * 2)
//                }
//              }
//              idsToRelevancy.updatedWith(page) {
//                case Some(v) => Some(v + tf * idf)
//              }
//              idsToRelevancy + (if (idsToRelevancy.contains(term)) page ->
//              idsToRelevancy += idsToRelevancy.get(page).map(score => page -> score + tf * idf).getOrElse(page -> tf * idf)

              if (idsToRelevancy.contains(page)) {
                  idsToRelevancy(page) = idsToRelevancy(page) + idf * tf
              }
              else {
                idsToRelevancy(page) = idf * tf
              }

              if (this.usePageRank) {
                idsToRelevancy(page) = idsToRelevancy(page) * idsToPageRank(page)
              }
            }
          }
        }
      }
      else {}
    }

    // sort relevancy scores in descending order
    val sortedScores: Array[Int] = idsToRelevancy.keys.toArray.sortWith(idsToRelevancy(_) > idsToRelevancy(_))

    if (sortedScores.nonEmpty) {
      // if sorted scores are not empty, print our results
      printResults(sortedScores)
    } else {
      // if no result, print an informative message
      println("Oops, we couldn't find any results for your query!")
    }
  }

  /**
   * Format and print up to 10 results from the results list
   *
   * @param results - an array of all results to be printed
   */
  private def printResults(results: Array[Int]) {
    for (i <- 0 until Math.min(10, results.size)) {
      println("\t" + (i + 1) + " " + idsToTitle(results(i)))
    }
  }

  /*
   * Reads in the text files.
   */
  def readFiles(): Unit = {
    FileIO.readTitles(titleIndex, idsToTitle)
    FileIO.readDocuments(documentIndex, idsToMaxFreqs, idsToPageRank)
    FileIO.readWords(wordIndex, wordsToDocumentFrequencies)
  }

  /**
   * Starts the read and print loop for queries
   */
  def run() {
    val inputReader = new BufferedReader(new InputStreamReader(System.in))

    // Print the first query prompt and read the first line of input
    print("search> ")
    var userQuery = inputReader.readLine()

    // Loop until there are no more input lines (EOF is reached)
    while (userQuery != null) {
      // If ":quit" is reached, exit the loop
      if (userQuery == ":quit") {
        inputReader.close()
        return
      }

      // Handle the query for the single line of input
      query(userQuery)

      // Print next query prompt and read next line of input
      print("search> ")
      userQuery = inputReader.readLine()
    }

    inputReader.close()
  }
}

object Query {
  def main(args: Array[String]) {
    try {
      // Run queries with page rank
      var pageRank = false
      var titleIndex = 0
      var docIndex = 1
      var wordIndex = 2
      if (args.size == 4 && args(0) == "--pagerank") {
        pageRank = true;
        titleIndex = 1
        docIndex = 2
        wordIndex = 3
      } else if (args.size != 3) {
        println("Incorrect arguments. Please use [--pagerank] <titleIndex> "
          + "<documentIndex> <wordIndex>")
        System.exit(1)
      }
      val query: Query = new Query(args(titleIndex), args(docIndex), args(wordIndex), pageRank)
      query.readFiles()
      query.run()
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}
