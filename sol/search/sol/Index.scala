package search.sol

import search.src.{FileIO, PorterStemmer, StopWords}

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

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word --> querier calculates tf and idf
  //  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]

  private val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")

  //--> this will return only the content within the [[ ]] i.e. "This is a [[hammer]]" will only return "hammer"
  // [[presidents|washington]] will return "presidents|washington"
  //private val linkContentPattern = new Regex("""(?<=\[\[).+?(?=])""")

  private val regexLink = """\[\[[^\[]+?\]\]"""
  private val regexMetaPage = """\[\[[^\[]+?\:[^\[]+?\]\]"""
  private val regexPipeLink = """\[\[[^\[]+?\|[^\[]+?\]\]"""

  // ! Backup regexes for links
  // new Regex("""[^\[\|\]]+""") --> pipe links
  // new Regex("""[^\[\]]+""") --> normal totalLinks
  // new Regex("""[^\[\]]+""") --> metalinks

  /**
    * A helper function that populates the termsToIdFreq hashMap while stemming input words and removing stop words
    *
    * @param word                - a word from a page in the corpus
    * @param id                  - the id of the page this word appears in
    * @param termsToFreqThisPage - a HashMap of stemmed terms to their frequency on this page (input id)
    */
  private def termsToIdFreqHelper(word: String, id: Int, termsToFreqThisPage: HashMap[String, Int]): Unit = {
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
    val rootNode: Node = xml.XML.loadFile(inputFile)

    for (page <- rootNode \ "page") {
      // extract id
      val id: Int = (page \\ "id").text.trim().toInt
      // extract title
      val title: String = (page \\ "title").text.trim()
      // add id & title to hashmap
      idsToTitle(id) = title
      titleToIds(title) = id
    }


    for (page <- rootNode \ "page") {
      // extract id
      val id: Int = (page \\ "id").text.trim().toInt
      // get concatenation of all text in the page
      //concatenate title to body --> not include ids in pageString
      val body: String = (page \\ "text").text.trim()
      val pageString: String = (page \\ "title").text.trim().concat(body)
      // remove punctuation and whitespace, matching all words including pipe links and meta pages
      val matchesIterator = regex.findAllMatchIn(pageString)
      // convert to list (each element is a word of the page)
      val matchesList = matchesIterator.toList.map { aMatch => aMatch.matched }

      // hashmap to store terms to their frequency on this page (intermediate step for termsToIdFreq)
      val termsToFreqThisPage = new scala.collection.mutable.HashMap[String, Int]

      // create a word list to store future words
      // ! this is an immutable list. Var only affects environment names
      var wordList: List[String] = List()

      // for all words on this page
      for (word <- matchesList) {

        // if our word is a link
        if (word.matches(regexLink)) {
          // use regex to get rid of the [[ ]] and replace them with empty string -->please check if regex correct
          val linkTitle: String = word.replaceAll("""/[\[\]']+/""", "''")
          // case 1: pipe link
          if (word.matches(regexPipeLink)) {
            //using split: Leaders|US Presidents|JFK --> Array["Leaders", "US Presidents", "JFK"]; see Piazza post @1358 to see what to keep
            //summary: the first item in the array gets stored in idToLinkIds, not in wordlist; the second item (the one right behind the second pipe)
            // gets stored in wordlist and not idToLinkIds; the later items are ignored
            val linkName = linkTitle.split("\\|")(0) //e.g. "Leaders"
            val addToWords = linkTitle.split("\\|")(1) //e.g. "US Presidents"
<<<<<<< HEAD
            // ! can't add element to an immutable list
            wordList += addToWords
=======
            wordList = wordList :+ addToWords
>>>>>>> 292ff0f143004b084c94740514c8fa1cc3ba83b7
            // adding the id of the link to idToLinkIds
            if (titleToIds.keySet.contains(linkName)) {
              idToLinkIds(id) + titleToIds(linkName)
            }
          }
          // case 2: meta-page link
          else if (word.matches(regexMetaPage)) {
            //using regex: Category: Computer Science --> Array["Category", "Computer" ,"Science"]
            val words = regex.findAllMatchIn(word).toList.map { aMatch => aMatch.matched } //--> "Category: Computer Science" => ["Category", "Computer" ,"Science"]
            // merge the list with wordlist
            wordList = wordList ++ words
            // adding the id of the link to idToLinkIds
            if (titleToIds.keySet.contains(linkTitle)) {
              idToLinkIds(id) + titleToIds(linkTitle)
            }
          } //case 3: normal link
          else {
            // add the title to the wordlist
            wordList = wordList :+ linkTitle
            // adding the id of the link to idToLinkIds
            if (titleToIds.keySet.contains(linkTitle)) {
              idToLinkIds(id) + titleToIds(linkTitle)
            }
          }
        }
        // our word is not a link
        else {
<<<<<<< HEAD
          // ! more efficient to pass directly to helper function than add to intermediate list of values
          wordList += word
=======
          wordList = wordList :+ word
>>>>>>> 292ff0f143004b084c94740514c8fa1cc3ba83b7
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

      // populate termsToIdFreq map (to be stemmed and stopped)
      for (linkWord <- wordList) {
        termsToIdFreqHelper(linkWord, id, termsToFreqThisPage)
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

  // store the total number of pages
  private val totalPageNum: Int = idToLinkIds.size

  // create an array that contains all page ids -->useful later for checking if a link exists or not
  private val allIds: Array[Int] = new Array[Int](idToLinkIds.size)
  for (id <- idToLinkIds.keys) {
    allIds :+ id
  }

  /**
    * A method that generates the weight distribution matrix
    * @return - a HashMap that maps the id of a page to the weight distribution of its different links in the form of
    *         another hashMap that maps the id of the link to the weight
    */
  private def weightMatrix(): HashMap[Int, HashMap[Int, Double]] = {
    //initialize global constants
    val epsilon: Double = 0.15
    var weight: Double = 0.0
    //initialize empty outer hashmap
    val outerHashMap = new HashMap[Int, HashMap[Int, Double]]()
    for ((j, links) <- idToLinkIds) {
      // total number of links in page j
      val totalLinks: Int = idToLinkIds(j).size
      //initiating new inner hashMap
      val innerHashMap = new HashMap[Int, Double]()
      // for each link in the page j
      for (k <- links) {
        //if the page doesn't link to anything --> link once to everywhere except itself
        if (totalLinks == 0) {
          weight = epsilon / totalPageNum + (1 - epsilon) / (totalPageNum - 1)
        }
        //normal case: such as A and C in the example above
        // --> if the links of a link contains this page & the page is not referring to itself, calculate weight
        if (idToLinkIds(k).contains(j) && (j != k)) {
          weight = epsilon / totalPageNum + (1 - epsilon) / totalLinks
        }
        // links from a page to itself or link to pages outside corpus -> ignored
        if ((j == k) || !allIds.contains(k)) {
          weight = 0.0
        }
        // otherwise
        else {
          weight = epsilon / totalPageNum
        }
        //populate the inner hashmap with weights corresponding to link k
        innerHashMap(k) = weight
      }
      // populate the outer hashmap with inner weight distribution for each link k in corresponding page j
      outerHashMap(j) = innerHashMap
    }
    outerHashMap
  }

  //assign weight distribution to be the hashmap generated in weightMatrix()
  private val weightDistribution: HashMap[Int, HashMap[Int, Double]] = weightMatrix()

  /**
    * A helper function calculating the distance between two arrays, will be used in pageRank()
    * @param previous - the array from the previous iteration
    * @param current - the array from this iteration
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
    * @return - a HashMap mapping each id to the rank score that page receives
    */
  private def pageRank(): HashMap[Int, Double] = {
    // initialize previous to be an array of n zeros (previous represents the array in the previous iteration)
    var previous: Array[Double] = Array.fill[Double](totalPageNum)(0)
    // initialize current to be an array of n 1/n (previous represents the array in this iteration), let n be 1/total number of pages
    var current: Array[Double] = Array.fill[Double](totalPageNum)(1 / totalPageNum)
    // while distance between arrays from consecutive iterations is greater than a constant (we set the constant to be 0.0001 for now)
    while (distance(previous, current) > 0.0001) {
      // the previous array assigned as the current array
      previous = current
      // for id_j in all ids
      for (j <- allIds) {
        // reset current array to be zero
        current(j) = 0.0
        // for id_k in all ids
        for (k <- allIds) {
          // refer to handout & the table that calculates rank of a, b, c
          current(j) = current(j) + weightDistribution(j)(k) * previous(k)
        }
        // set the page rank at id_j to be the rank score calculated for the links score combined
        idsToPageRank(j) = current(j)
      }
    }
    idsToPageRank
  }

}

object Index {
  def main(args: Array[String]): Unit = {
    // create instance of indexer, passing input file into constructor
    val indexer = new Index(args(0))
    // call parsing function which populates the idsToTitle, idsToMaxCounts, and termsToIdFreq hashmaps
    indexer.parsing()
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
