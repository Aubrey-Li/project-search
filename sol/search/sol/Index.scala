package search.sol

import search.src.FileIO
import search.src.StopWords.isStopWord
import search.src.PorterStemmer.stem
import scala.collection.mutable.HashSet
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

  // page ids to Set of link ids (we use HashSet to avoid duplicates in links)
  private val idToLinkIds = new HashMap[Int, HashSet[Int]]

  // page title mapping to its id
  private val titleToIds = new HashMap[String, Int]

  //create getter
  def getidsToTitle(): HashMap[Int, String] = {
    idsToTitle
  }

  def getTermsToIdFreq(): HashMap[String, HashMap[Int, Double]] = {
    termsToIdFreq
  }
  def getIdsToMaxCounts(): HashMap[Int, Double] = {
    idsToMaxCounts
  }
  def getIdsToPageRank(): HashMap[Int, Double] = {
    idsToPageRank
  }
  def getIdToLinkIds(): HashMap[Int, HashSet[Int]] = {
    idToLinkIds
  }
  def getTitleToIds(): HashMap[String, Int] = {
    titleToIds
  }

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word --> querier calculates tf and idf
  //  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]

  // regex to remove white space and punctuation
  private val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")

  private val regexLink = """\[\[[^\[]+?\]\]"""
  private val regexPipeLink = """\[\[[^\[]+?\|[^\[]+?\]\]"""

  // regexes to process links in the helper functions (removes square brackets)
  private val regexPipeLinkHelper = new Regex("""[^\[\|\]]+""")
  private val regexNormalLinkHelper = new Regex("""[^\[\]]+""")

  /**
   * A helper function that populates the termsToIdFreq hashMap while stemming input words and removing stop words
   *
   * @param word                - a word from a page in the corpus
   * @param id                  - the id of the page this word appears in
   * @param termsToFreqThisPage - a HashMap of stemmed terms to their frequency on this page (input id)
   */
    //TODO: ONE THING THAT TAKES SPACE IN THIS PROCESS IS TO STORE A termsToFreqThisPage every page
    //TODO: consider spliting the function that, instead of storing the number of times each word appears this page, consider
    //todo: not creating termsToFreqThisPage, and just use a loop to keep on updating the ???
  private def termsToIdFreqHelper(word: String, id: Int, termsToFreqThisPage: HashMap[String, Int]): Unit = {
    // if not stop word, stem
    if (!isStopWord(word)) {
      val term = stem(word)
      // if stemmed version is not a stop word
      if (!isStopWord(term)) {
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
        termsToIdFreq(term) = HashMap(id -> 1)
      }
      // if stop word, do nothing
    } else {}
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
    val pipeIterator: Iterator[Regex.Match] = regexPipeLinkHelper.findAllMatchIn(linkString)

    // convert to list (each element is a word of the page)
    val LinkWordStrings: List[String] = pipeIterator.toList.map { aMatch => aMatch.matched }

    // extract the link name
    val linkName = LinkWordStrings(0) //e.g. "Leaders"
    // string after the pipe character (words to process)
    val addToWords = LinkWordStrings(1) //e.g. "US Presidents"

    // remove white space and punctuation
    val matchesIteratorWords = regex.findAllMatchIn(addToWords)
    // convert to list (each element is a word of the page)
    val nonLinkWords = matchesIteratorWords.toList.map { aMatch => aMatch.matched }

    // adding the id of the link to idToLinkIds
    if (titleToIds.keySet.contains(linkName)) {
      if (!idToLinkIds.keySet.contains(id)) {
        idToLinkIds(id) += titleToIds(linkName)
      } else {
        idToLinkIds += (id -> HashSet(titleToIds(linkName)))
      }
    }
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
    val matchesIteratorAll: Iterator[Regex.Match] = regexNormalLinkHelper.findAllMatchIn(linkString)

    // convert to list, there should just be one long string in the list
    val LinkWordStrings: List[String] = matchesIteratorAll.toList.map { aMatch => aMatch.matched }

    // parse the long string to words
    val matchesIteratorWords: Iterator[Regex.Match] = regex.findAllMatchIn(LinkWordStrings(0))
    // convert to list
    val nonLinkWords: List[String] = matchesIteratorWords.toList.map { aMatch => aMatch.matched }

    // adding the id of the link to idToLinkIds
    if (titleToIds.keySet.contains(LinkWordStrings(0))) {
      idToLinkIds(id) += titleToIds(LinkWordStrings(0))
    }
    nonLinkWords
  }

  /**
   * A function that parse the document and creates a HashMap with its id as key and the list of words in the corpus
   * as the value
   *
   * @return hashmap of page id to List of words in that page (with punctuation, whitespace removed)
   *         and links, pipe links, and metapages parsed to remove square brackets
   */
  private def parsing(): Unit = {
    val rootNode: Node = xml.XML.loadFile(inputFile)

    for (page <- rootNode \ "page") {
      // extract title
      val title: String = (page \ "title").text.trim()
      // extract id
      val id: Int = (page \\ "id").text.trim().toInt
      // add id & title to hashmap
      idsToTitle(id) = title
      titleToIds(title) = id
      idToLinkIds(id) = new HashSet[Int]()
      idsToPageRank(id) = 0.0
    }


    for (page <- rootNode \ "page") {
      // extract id
      val id: Int = (page \\ "id").text.trim().toInt
      // get concatenation of all text in the page
      //concatenate title to body --> not include ids in pageString
      val pageString: String = (page \\ "title").text.trim().concat((page \\ "text").text.trim())
      // remove punctuation and whitespace, matching all words including pipe links and meta pages & convert to list
      val matchesList = regex.findAllMatchIn(pageString).toList.map { aMatch => aMatch.matched }

      // hashmap to store terms to their frequency on this page (intermediate step for termsToIdFreq)
      val termsToFreqThisPage = new HashMap[String, Int]

      // for all words on this page
      for (word <- matchesList) {

        // if our word is a link
        if (word.matches(regexLink)) {
          // case 1: pipe link
          if (word.matches(regexPipeLink)) {

            // extract word(s) to process (omit underlying link)
            // pass word(s) to termsToIdFreq helper
            for (linkWord <- pipeLinkHelper(word, id)) {
              // populate termsToIdFreq map (to be stemmed and stopped)
              termsToIdFreqHelper(linkWord, id, termsToFreqThisPage)
            }

          } //case 2 and 3: normal link or meta page
          else {
            // extract word(s) to process (omit underlying link)
            // pass word(s) to termsToIdFreq helper
            for (linkWord <- normalLinkHelper(word, id)) {
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
  // The matrix take the form of a nested HashMap that maps id of a page to a hashmap of link ids to its weight
  // a visual representation: (see graph in handout with the A, B, C)
  //    A       B       C
  // A  0       0.475   0.9
  // B  0.475   0       0.475
  // C  0.475   0.475   0
  //--> HashMap {A_id, {B_id, 0.475; C_id, 0.475};
  //             B_id, {A_id, 0.475; C_id, 0.475};
  //             C_id, {A_id, 0.9; B_id, 0.475};
  //            }
  //2. Use the matrix in page rank algorithm to calculate the rank of each page after multiple iterations such that the
  //distance between arrays in consecutive iterations are smaller than a constant

  //populate the HashMaps
  parsing()
  // store the total number of pages & set global constant epsilon
  private val totalPageNum: Int = idToLinkIds.size
  val epsilon: Double = 0.15

  /**
   * A method that calculates weight
   *
   * @return - double representing the weight with a combination of page j and link k
   */
  private def calcWeight(linkPage: Int, page: Int): Double = {
    // total number of links in page j
    val totalLinks: Int = idToLinkIds(page).size
    //if the page doesn't link to anything --> link once to everywhere
    if (totalLinks == 0) {
      // weight equals to:
      epsilon / totalPageNum + (1 - epsilon) / (totalPageNum - 1)
    } else { // if the page has links -->linkPage is valid
      // --> if the the links of the page contains link k & the page is not referring to itself, calculate weight
      if (idToLinkIds(page).contains(linkPage) && (page != linkPage)) {
        epsilon / totalPageNum + (1 - epsilon) / totalLinks
      }
      //if the page refers to itself and doesn't exists as a link in the page
      else if (page == linkPage && !idToLinkIds(page).contains(linkPage)) {
        epsilon / totalPageNum
      }
      // links from a page to itself or link to pages outside corpus -> ignored, weight = 0
      else if ((idToLinkIds(page).contains(linkPage) && page == linkPage) || !idToLinkIds.keySet.contains(linkPage)) {
        0.0
      }
      else {
        epsilon / totalPageNum
      }
    }
  }


  /**
   * A helper function calculating the distance between two arrays, will be used in pageRank()
   *
   * @param previous - the array from the previous iteration
   * @param current  - the array from this iteration
   * @return a Double representing the Euclidean distance between the arrays
   */
  private def distance(previous: Array[Double], current: Array[Double]): Double = {
    // euclidean distance = sqrt of (sum of all (differences)^2)--see handout
    var differenceSum: Double = 0.0
    for (i <- 0 until previous.length - 1) {
      differenceSum += Math.pow(previous(i) - current(i), 2)
    }
    Math.sqrt(differenceSum)
  }

  /**
   * the page rank algorithm that calculates the ranking score for each page
   *
   * @return - a HashMap mapping each id to the rank score that page receives
   */
  private def pageRank(): HashMap[Int, Double] = {
    // initialize previous to be an array of n zeros (previous represents the array in the previous iteration)
    var previous: Array[Double] = Array.fill[Double](totalPageNum + 1)(0)
    // initialize current to be an array of n 1/n (previous represents the array in this iteration), let n be 1/50 (randomly chosen)
    val current: Array[Double] = Array.fill[Double](totalPageNum + 1)(1.0 / 50)
    // while distance between arrays from consecutive iterations is greater than a constant (we set the constant to be 0.0001 for now)
    while (distance(previous, current) > 0.0001) {
      // the previous array assigned as the current array
      previous = current
      // for j between 0 and total page number
      for (j <- 0 to totalPageNum) {
        //if j is an id
        if (idsToPageRank.keySet.contains(j)) {
          // reset current array to be zero
          current(j) = 0.0
          // for k between 0 and total page number
          for (k <- 0 to totalPageNum) {
            // if k is an id
            if (idsToPageRank.keySet.contains(k)) {
              current(j) = current(j) + calcWeight(j, k) * previous(k)
            }
          }
          // set the page rank at id_j to be the rank score calculated for the links score combined
          idsToPageRank(j) = current(j)
        }
      }
    }
    idsToPageRank
  }

}

//TO RUN:
// fill in the file name with relative path, titles.txt, docs.txt, and word.txt separated by space under edit configuration, program argument
object Index {
  def main(args: Array[String]): Unit = {
    // create instance of indexer, passing input file into constructor
    val indexer = new Index(args(0))
    // call pageRank function which populates the idsToPageRank hashmap
    indexer.pageRank()

    // generate titles.txt
    FileIO.printTitleFile(args(1), indexer.idsToTitle)
    // generate docs.txt
    FileIO.printDocumentFile(args(2), indexer.idsToMaxCounts, indexer.idsToPageRank)
    // generate words.txt
    FileIO.printWordsFile(args(3), indexer.termsToIdFreq)
  }
}
