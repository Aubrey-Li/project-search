package search.sol

import java.io.{FileNotFoundException, IOException}

import search.src.FileIO
import search.src.StopWords.isStopWord
import search.src.PorterStemmer.stemArray
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
  // extract the node sequence of pages from the xml file
  private var pages: NodeSeq = xml.XML.loadFile(inputFile) \ "page"

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


  // initialize the total number of pages
  private var totalPageNum: Int = 0

  // set global constant epsilon
  val epsilon: Double = 0.15

  // regex to remove white space and punctuation
  private val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")

  // regex for links [[ ]] only
  private val regexLink = """\[\[[^\[]+?\]\]"""

  // regex link parser
  private val regexLinkParser = new Regex(regexLink)

  // regex for pipelinks [[ | ]] only
  private val regexPipeLink = """\[\[[^\[]+?\|[^\[]+?\]\]"""


  // regexes to process links in the helper functions (removes square brackets)
  private val regexPipeLinkHelper = new Regex("""[^\[\|\]]+""")
  private val regexNormalLinkHelper = new Regex("""[^\[\]]+""")

  //-------------------------------------------------------------------------------------------------------
  // prepare for files needed for titles.txt
  //-------------------------------------------------------------------------------------------------------

  /**
    * A helper function that populates idToTitles
    */
  protected def populateIdToTitle(): Unit = {
    // populate idsToTitle hashMap
    for (page <- pages) {
      // extract title
      val title: String = (page \ "title").text.trim()
      // extract id
      val id: Int = (page \ "id").text.trim().toInt
      // add id & title to hashmap
      idsToTitle(id) = title
    }
  }

  //-------------------------------------------------------------------------------------------------------
  // Populates hashmaps related to words in a page
  //-------------------------------------------------------------------------------------------------------

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

    // string immediately after the first pipe character --> to add to words (words to process)
    val addToWords = LinkWordStrings(1)

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
    * A function that parse the document and generates a termsToIdFreq and idToMaxCount hashmaps
    */
  protected def parsing(): Unit = {

    for (page <- pages) {
      // extract id
      val id: Int = (page \ "id").text.trim().toInt
      // (all steps combined in one line to save memory!)
      // 1. get concatenation of all text in the page
      // 2. concatenate title to body, excluding ids
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
      // clear termsToFreqThisPage to release memory
      termsToFreqThisPage.clear()

      // remove this node after each iteration, for the purpose of saving memory
      val removeIt = new RewriteRule {
        override def transform(n: Node): NodeSeq = n match {
          case e: Elem => NodeSeq.Empty
          case n => n
        }
      }
      new RuleTransformer(removeIt).transform(page)
    }
  }

  //-------------------------------------------------------------------------------------------------------
  // Page rank
  //-------------------------------------------------------------------------------------------------------

  /**
    * A helper function that populates IdToLinkIds hashmap
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

          // extract the link name (for pipelink, all content before the first pipe)
          val linkName = LinkWordStrings.head

          // adding the id of the link to idToLinkIds
          if (titleToIds.keySet.contains(linkName)) {
            if (!idToLinkIds.keySet.contains(id)) {
              idToLinkIds(id) += titleToIds(linkName)
            } else {
              idToLinkIds += (id -> HashSet(titleToIds(linkName)))
            }
          }
        } //case 2 and 3: normal link or meta page (both fall under the same logic, so merged as one case to handle
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
    * A helper that initializes idToPageRank
    */
  private def populatePageRank(): Unit = {
    // populate idsToPageRank hashmaps
    for (page <- pages) {
      // extract id
      val id: Int = (page \ "id").text.trim().toInt
      // add id & title to hashmap
      idsToPageRank(id) = 0.0
    }
    totalPageNum = idsToPageRank.size
  }

  // below are the implementation for calculating page rank

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
    * Applying the page rank algorithm that calculates the ranking score for each page, populates idToPageRank hashmap
    */
  protected def pageRank(): Unit = {
    // initialize previous to be an array of n zeros (previous represents the array in the previous iteration)
    var previous: Array[Double] = Array.fill[Double](totalPageNum + 1)(0)
    // initialize current to be an array of n 1/n (previous represents the array in this iteration),
    // let n be 1/50 (randomly chosen)
    val current: Array[Double] = Array.fill[Double](totalPageNum + 1)(1.0 / 50)
    // while distance between arrays from consecutive iterations is greater than a constant
    // (we set the constant to be 0.0001 for now)
    while (distance(previous, current) > 0.001) {
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
              current(j) += calcWeight(j, k) * previous(k)
            }
          }
          // set the page rank at id_j to be the rank score calculated for the links score combined
          idsToPageRank(j) = current(j)
        }
      }
    }
  }

}

/**
  * TO RUN:
  * fill in the file name with relative path, titles.txt, docs.txt, and word.txt separated by space under
  * edit configuration, program argument
  */
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

        // populates termToIdFreq
        indexer.parsing()
        // generate words.txt
        FileIO.printWordsFile(args(3), indexer.termsToIdFreq)
        indexer.termsToIdFreq.clear()

        // populates IdToLinkIds, page rank
        indexer.populateIdToLinkIds()
        indexer.titleToIds.clear()
        indexer.populatePageRank()
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

//---------------------------------------------------------------------------------
// Testing (Please refer to results in readMe
//---------------------------------------------------------------------------------
//object Index {
//  def main(args: Array[String]): Unit = {
//    val indexer = new Index(args(0))
//    // tests for SmallWiki
//    println("*** tests ***")
//    indexer.populateIdToTitle()
//    println(indexer.idsToTitle(1))
//    indexer.populateIdToLinkIds()
//    println(indexer.idToLinkIds(1))
//    indexer.parsing()
//    indexer.populatePageRank()
//    indexer.pageRank()
//    println(indexer.idsToMaxCounts(1))
//    println(indexer.idsToPageRank(4))
//    println(indexer.termsToIdFreq("words"))
//  }
//}
