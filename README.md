# project-search
Implementing a simple version of google search

---------------------------------------------------------------------------------
INSTRUCTIONS FOR USE
---------------------------------------------------------------------------------
This search engine consists of an indexer and a querier. The indexer generates three text files - docs.txt, words.txt,
and titles.txt - that the querier uses to service search requests. A user will interact primarily with the querier,
which when run produces an interactive repl shell prompting the user to input one or more search terms, which when entered,
returns a top 10 list of the most relevant results. If no relevant search results are found, an informative message is
printed to the console.


---------------------------------------------------------------------------------
DESIGN OVERVIEW
---------------------------------------------------------------------------------
Before any querying can take place, a user must run the indexer on a corpus of xml files to generate the text files the
querier will draw upon to service search requests. In order to minimize memory use and improve readability, we decided
to populate each of our hashmaps in separate functions, clearing them from memory as soon as they cease to be needed.
A high level overview of our functions and their purpose can be found below:

populateIdToTitle() → iterates over the corpus, mapping ids to titles
parsing() → stems and removes stop words, mapping terms to their frequency on each page
→ uses termsToIdFreqHelper(), pipeLinkHelper(), and normalLinkHelper() to extract terms from links and populate the hashmap
populateMaxCounts() -> maps page ids to their max frequency
→ uses termsMaxHelper() to populate the hashmap
populateIdToTitle() → maps ids to document titles
populatePageRank() → maps document ids to their page rank

Using the termsToIdFreq hashmap, we generate words.txt, which contains all terms found in the corpus along with the
frequency of each term in every page.

Using the idsToTitle hashmap, we generate titles.txt, which contains the titles and page ids of all documents in the corpus.

Using the idsToMaxCounts and idsToPageRank hashmaps, we generate docs.txt, which contains the page rank and maximum term
frequency of each page.

After the indexer has run, the querier uses the three text files to populate several hashmaps.
Given a user’s text input, our query removes stop words and stems all words of query, yielding an array of terms.
For each term, we use the information in our hashmaps to calculate term frequency and inverse document frequency,
populating a hashmap of page ids to their relevancy score. Finally, we sort the result and print the top 10 results to
the console, or an informative error message if no results were found.

query() → generates an array of results, sorted by relevance, and prints them to the console
idfTfHelper() → calculates term frequency and inverse document frequency given a term and a page id


---------------------------------------------------------------------------------
EXTRA FEATURES
---------------------------------------------------------------------------------
We supported page IDs to be random and inconsecutive integers. (please refer to testing section for details)


---------------------------------------------------------------------------------
KNOWN BUGS
---------------------------------------------------------------------------------
No known bugs, aside from the fact that the indexer does not currently run with under 1gb of memory using BigWiki.
The results generated differ slightly from those shown in the demo.


---------------------------------------------------------------------------------
TESTING
---------------------------------------------------------------------------------
To test our program, we divide the testing to a unit test suite for Indexer and a system test approach for the Querier.
In the test suite for Indexer, we majorly test if the hashmaps generated in the indexer align with our expectations,
this includes testing for HashMap idToTitles, idToMaxCounts, and termsToIdFreq. A test for pageRank would be completed
through inputting the PageRankWiki.xml and check that page 100 receives a much higher score than the others.
In the system test for Querier, we majorly test to see if the query we input yields the desired or relevant results given
the three files we generate from Indexer.

Our testing wiki:
<xml>
    <page>
    <id>
        0
    </id>
    <title>
        title1
    </title>
    <text>
        Good Good me! What a lovely drop of tea this is for thee
        [[title2]]
    </text>
    </page>
    <page>
        <id>
            1
        </id>
        <title>
           title2
        </title>
        <text>
            A drop of tea for thee.
            [[title1]] [[title3|tea|boba]]
        </text>
    </page>
    <page>
        <id>
            2
        </id>
        <title>
           title3
        </title>
        <text>
            Hello I am text3. Goodness me! What a lovely day it is.
            [[title2]] [[Category: title4]]
        </text>
    </page>
    <page>
        <title>
            page4
        </title>
        <id>
            4
        </id>
        <text>
            [[page0|page2]] rank rank rank rank rank [[title1]] [[title2]] [[title3]] [[title1]]
        </text>
    </page>
    <page>
        <title>
            page5
        </title>
        <id>
            5
        </id>
        <text>
            [[dead]] some coding a day
        </text>
    </page>
    <page>
        <title>
            page7
        </title>
        <id>
            7
        </id>
        <text>
        </text>
    </page>
</xml>


Indexer test suite result analysis:

Since our methods generating data structure and the hashmaps we need to populate are private or protected,
we test our hashmaps by examining the files it generates.

Tests for idToTitles:
In our wikiTest1.xml file, there are 3 articles with title1-3 and ids 0-2; after running our indexer,
we get the output of id 0 corresponding to title1, id 1 corresponding to title2, id 2 corresponding to title 3,
which is in line with our expected outcome. Similarly, in our test for SmallWiki.xml, all the titles are mapped
correctly with their id in the titles.txt file. The idToTitles also works for other languages like Thai, as we see
in the SRC file, which is in line with our expectation, since the title of the article is not processed and goes
directly to the hashmap as a string. (Id includes inconsecutive numbers)

Result according to expectation:
2::title3
5::page5
4::page4
7::page7
1::title2
0::title1


Tests for termsToIdFreq:

page7 7 1.0
rank 4 5.0
thee 1 1.0 0 1.0
title1 4 2.0 1 1.0 0 1.0
Good 2 1.0 0 2.0
tea 1 2.0 0 1.0
text3 2 1.0
title4 2 1.0
drop 1 1.0 0 1.0
thi 0 1.0
dai 2 1.0 5 1.0
Hello 2 1.0
love 2 1.0 0 1.0
title3 2 1.0 4 1.0
code 5 1.0
page5 5 1.0
page2 4 1.0
title2 2 1.0 4 1.0 1 1.0 0 1.0
dead 5 1.0
page4 4 1.0
Category 2 1.0

From the word frequencies for different pages we generate from hashmap termsToIdFreq, we see that all the words in line
with our expectation are documented. Cases:
1. Titles such as title1, title2, title3 are documented as words and they appear in the corresponding id location in
order of id 0 to 2.

2. The terms in the hashmap are fully removed of stop words such as a, of, is, for, etc. and punctuations such as comma,
period, exclamation mark.

3. The terms in the hashmap are fully stemmed. Examples: dai (for day), love (for lovely)

4. The occurrences of words are accurately documented. Example: love (from lovely) appears in page title1 and title3,
the ids of the corresponding pages are accurately documented (id: 2, occurrence: 1.0; id: 0: occurrence: 1.0)

5. Normal links are accurately incorporated in the termsToIdFreq hashmap: for the first page, [[title2]] is documented
to appear once, and “title2” inside the [[ ]] are extracted during representation. For the second page: “title1” in
[[title1]] and “tea” in [[title3|tea|boba]] is documented while title3 and boba are not treated as words in the text
(according to handout and piazza specifications). For the third page, “title2” in [[title2]] and “Category” and “title4”
in [[Category: title4]] goes into words, which we can also locate in the words.txt (Category 2 1.0 and title4 2 1.0).

6. Words that have zero appearance are not documented.

Tests for idToMaxCounts:

Results:
Id-2 Max-1.0 (corresponding to --all words appear exactly once in this page)
Id-5 Max-1.0 (corresponding to --all words appear exactly once in this page)
Id-4 Max-5.0 (corresponding to -- “rank” appears 5 times)
Id-7 Max-1.0 (corresponding to -- all words appear exactly once in this page)
Id-1 Max-2.0 (corresponding to -- “tea” appears 2 times)
Id-0 Max-2.0 (corresponding to -- “Good” appears 2 times)

Results of MaxCount for PageRankWiki:
99 99 2.0
90 90 2.0
57 57 2.0
84 84 2.0
78 78 2.0
63 63 2.0
45 45 2.0
39 39 2.0
30 30 2.0
66 66 2.0
51 51 2.0
2 2 2.0
87 87 2.0
72 72 2.0
5 5 2.0
48 48 2.0
33 33 2.0
69 69 2.0
27 27 2.0
54 54 2.0
12 12 2.0
60 60 2.0
8 8 2.0
15 15 2.0
42 42 2.0
36 36 2.0
98 98 2.0
21 21 2.0
71 71 2.0
92 92 2.0
18 18 2.0
86 86 2.0
24 24 2.0
74 74 2.0
89 89 2.0
95 95 2.0
53 53 2.0
68 68 2.0
80 80 2.0
41 41 2.0
35 35 2.0
83 83 2.0
56 56 2.0
62 62 2.0
7 7 2.0
77 77 2.0
59 59 2.0
1 1 2.0
17 17 2.0
50 50 2.0
44 44 2.0
23 23 2.0
38 38 2.0
65 65 2.0
47 47 2.0
26 26 2.0
4 4 2.0
11 11 2.0
32 32 2.0
14 14 2.0
97 97 2.0
82 82 2.0
29 29 2.0
20 20 2.0
79 79 2.0
85 85 2.0
70 70 2.0
64 64 2.0
91 91 2.0
46 46 2.0
94 94 2.0
52 52 2.0
67 67 2.0
73 73 2.0
100 92 1.0 83 1.0 23 1.0 95 1.0 77 1.0 86 1.0 50 1.0 59 1.0 41 1.0 32 1.0 68 1.0 53 1.0 62 1.0 35 1.0 44 1.0 8 1.0 17 1.0 26 1.0 80 1.0 89 1.0 98 1.0 71 1.0 11 1.0 74 1.0 56 1.0 38 1.0 29 1.0 47 1.0 20 1.0 2 1.0 65 1.0 5 1.0 14 1.0 46 1.0 100 3.0 82 1.0 91 1.0 55 1.0 64 1.0 73 1.0 58 1.0 67 1.0 85 1.0 94 1.0 49 1.0 40 1.0 13 1.0 4 1.0 22 1.0 31 1.0 76 1.0 16 1.0 97 1.0 7 1.0 79 1.0 88 1.0 70 1.0 43 1.0 52 1.0 25 1.0 34 1.0 61 1.0 10 1.0 37 1.0 1 1.0 28 1.0 19 1.0 60 1.0 87 1.0 96 1.0 69 1.0 78 1.0 99 1.0 63 1.0 90 1.0 45 1.0 54 1.0 72 1.0 81 1.0 27 1.0 36 1.0 9 1.0 18 1.0 48 1.0 21 1.0 57 1.0 12 1.0 3 1.0 84 1.0 93 1.0 75 1.0 30 1.0 39 1.0 66 1.0 15 1.0 42 1.0 51 1.0 33 1.0 24 1.0 6 1.0
88 88 2.0
34 34 2.0
28 28 2.0
6 6 2.0
40 40 2.0
55 55 2.0
49 49 2.0
61 61 2.0
76 76 2.0
9 9 2.0
43 43 2.0
22 22 2.0
58 58 2.0
16 16 2.0
37 37 2.0
19 19 2.0
3 3 2.0
10 10 2.0
31 31 2.0
25 25 2.0
93 93 2.0
13 13 2.0
75 75 2.0
81 81 2.0
96 96 2.0

We can see that all of the numbers appear twice in each page (once in title and once in body) except for 100,
which appears on every page once including itself (where it appears 3 times). This corresponds to our expectation.


Tests for Page Rank:
For testing the page rank, we use the PageRankWiki.xml to check if the rank score of page 100 is significantly greater
than the others, since all other pages link to page 100.
92 2.0 4.816694734739249E-4
83 2.0 7.431889122861316E-4
23 2.0 0.0023992053217312638
95 2.0 3.937082298338805E-4
77 2.0 9.155826205790397E-4
86 2.0 6.564080428195127E-4
50 2.0 0.0016724248371924172
59 2.0 0.001423544659700358
41 2.0 0.0019179652286419858
32 2.0 0.0021602106514825854
68 2.0 0.0011712792691354786
53 2.0 0.0015898380960543078
62 2.0 0.0013398345919937787
35 2.0 0.0020798255431560423
44 2.0 0.0018364867398690398
8 2.0 0.002790424093599755
17 2.0 0.0025567506944712093
26 2.0 0.002319898921502402
80 2.0 8.295798541928425E-4
89 2.0 5.692354858457202E-4
98 2.0 3.0534997103990854E-4
71 2.0 0.0010864305956211466
11 2.0 0.002712883468552157
74 2.0 0.0010011989556117783
56 2.0 0.0015068785977064493
38 2.0 0.0019990776147415256
29 2.0 0.002240234569960083
47 2.0 0.0017546404960096108
20 2.0 0.0024781553790090075
2 2.0 0.0029444616825000046
65 2.0 0.0012557466969164107
5 2.0 0.002867616309753729
14 2.0 0.002634992862059153
46 2.0 0.0017819635412797234
100 3.0 0.13956325422448604
82 2.0 7.720291315233007E-4
91 2.0 5.109019709880081E-4
55 2.0 0.0015345732849890747
64 2.0 0.0012838180812039222
73 2.0 0.0010296521606534368
58 2.0 0.0014513643476091623
67 2.0 0.0011994773542800116
85 2.0 6.853784330684357E-4
94 2.0 4.2307266891699995E-4
49 2.0 0.001699871205673694
40 2.0 0.001945043287363544
13 2.0 0.0026609953817469238
4 2.0 0.0028932698950538843
22 2.0 0.0024255615219349372
31 2.0 0.0021869253428862348
76 2.0 9.441642498347255E-4
16 2.0 0.0025828705771466904
97 2.0 3.3484694721345716E-4
7 2.0 0.002816193466964008
79 2.0 8.582904873082836E-4
88 2.0 5.983366346363377E-4
70 2.0 0.0011147559534900162
43 2.0 0.00186368701604918
52 2.0 0.0016174083443689796
25 2.0 0.002346374081039161
34 2.0 0.0021066608119512885
61 2.0 0.001367779844722045
10 2.0 0.002738769152592675
37 2.0 0.002026034005157278
1 2.0 0.0029700000000000048
28 2.0 0.0022668292257556916
19 2.0 0.0025043931543928207
60 2.0 0.0013956831796653928
87 2.0 6.273941318018383E-4
96 2.0 3.642996780221484E-4
69 2.0 0.0011430388234175374
78 2.0 8.869580545708047E-4
99 2.0 2.7580868303390325E-4
63 2.0 0.0013118473585096012
90 2.0 5.400906198543319E-4
45 2.0 0.001809245602074008
54 2.0 0.0015622264303341053
72 2.0 0.0010580626859834183
81 2.0 8.008260905288038E-4
27 2.0 0.002293383989657229
36 2.0 0.0020529499610782483
9 2.0 0.002764616008194365
18 2.0 0.0025305915732019785
48 2.0 0.0017272764046947413
21 2.0 0.0024518781879271237
57 2.0 0.0014791423060798539
12 2.0 0.0026869588977427895
3 2.0 0.0029188850000625402
84 2.0 7.143053678296135E-4
93 2.0 4.52393061440451E-4
75 2.0 9.727030067428461E-4
30 2.0 0.0022135999623428054
39 2.0 0.0019720807290882713
66 2.0 0.0012276331423918534
15 2.0 0.0026089512800861812
42 2.0 0.0018908464919067131
51 2.0 0.0016449372374040894
33 2.0 0.002133455827933775
24 2.0 0.0023728095279258346
6 2.0 0.0028419241863550564

Since the result for pagerankwiki shows that the rank score of page 100 is much higher than other rank scores
(mostly by 2 digits), we can show that our pagerank ranks the importance of the pages successfully.

Test for SmallWiki: ** For this part of the test, we temporarily loosened accessor requirements and set HashMaps to
public in order to perform testing; afterwards we changed back to private accessors.

Our tests:
def main(args: Array[String]): Unit = {
    val indexer = new Index(args(0))
    // tests for SmallWiki
    println("*** tests ***")
    indexer.populateIdToTitle()
    println(indexer.idsToTitle(1))
    indexer.populateIdToLinkIds()
    println(indexer.idToLinkIds(1))
    indexer.parsing()
    indexer.populatePageRank()
    println(indexer.idsToMaxCounts(1))
    println(indexer.idsToPageRank(1))
    println(indexer.termsToIdFreq("words"))
  }

Result:
*** tests ***
Anatopism
Set(34, 4)
6.0
0.022580392172204585
Map(81 -> 1.0)

This result aligns with our expectations.

-----------------------------------------------------
Querier system test result analysis:

**Below are the test results for BigWiki.xml:

Case 1: Not found, word
-->In the word.txt, there is no reference to the word "lolol",thus, the search doesn't return any pages.
search> lolol
Oops, we couldn't find any results for your query!

Case 2: Not found, stop word
--> In the word.txt, there are no references to the word 'an' and in addition, the query removes 'an',
because of the query has no references to the word 'an'.
search> an
Oops, we couldn't find any results for your query!

Case 3: enter empty space, apostrophes, or special char
search>
Oops, we couldn't find any results for your query!
search> '
Oops, we couldn't find any results for your query!
search> !!!!!!!!
Oops, we couldn't find any results for your query!

Case 4: Found, stemming verb establishing -> establish
search> establishing
	1 Foreign relations of Niger
	2 Transport in Ireland
	3 Pakistan Armed Forces
	4 Organized crime
	5 Politics of the Federated States of Micronesia
	6 IEEE 802.11
	7 Nigerian Armed Forces
	8 Politics of Nauru
	9 Economy of Greenland
	10 Economy of Haiti


 Case 5: Only has one link
--> In the word.txt it, the word "szkole" only has one reference to page with id 6620 and titled Poland, once.
We get only get one link back as expected and that link is first.
search> szkole
	1 Poland

Case 6: Less than 10 results
--> In the word.txt, the word "MGA" has less than 10 references. We get 7 links back ....
search> MGA
	1 Miranda warning
	2 Philippines
	3 Manx language
	4 Armed Forces of the Philippines
	5 Oath of office
	6 Multics
	7 Myasthenia gravis

Case 11: Number queries, less than 10 results
--> In the word.txt, the number "2376" has less than 10 references.
search> 2376
	1 Petroleum
	2 Grandmaster (chess)
	3 Jerry Falwell
	4 Knute Rockne
	5 Pterosaur

Case 12: 10+ results
--> In the word.txt , the word "berlin" has 10+ references, we get ten pages back.
search> berlin
	1 Platonic solid
	2 Film format
	3 Transport in Germany
	4 Cinema of Germany
	5 Paraphilia
	6 Germany
	7 East Germany
	8 Hera
	9 Frankfurt
	10 Prague


Case 13: More than ten, a word that’s  part of a title
search> poland
	1 Foreign relations of Latvia
	2 Transport in Poland
	3 Jean-Jacques Rousseau
	4 Cinema of Poland
	5 Politburo
	6 Foreign relations of Poland
	7 LGBT social movements
	8 Poland
	9 Krak?w
	10 Karl Marx


Case 14: Less than ten, a word that’s part of the title
 search> nerd
	1 Otaku
	2 Nerd
	3 Massachusetts Institute of Technology
	4 Forever Changes
	5 IKEA
	6 Moby
	7 James Parry
	8 Lincoln
	9 Neon Genesis Evangelion (anime)

Case 15: Two words
search> war loot
	1 Foreign relations of Niger
	2 Organized crime
	3 Pakistan Armed Forces
	4 Islamic eschatology
	5 Nigerian Armed Forces
	6 History of Nicaragua
	7 ?smet ?n?n?
	8 Mitosis
	9 History of Guatemala
	10 Cinema of the Soviet Union

Case 16: Three words
search> geography of latin america
	1 Pakistan Armed Forces
	2 Geography of Peru
	3 Economy of Haiti
	4 Geography of New Zealand
	5 Transport in Luxembourg
	6 Politics of Nicaragua
	7 Geography of Pakistan
	8 Economy of Peru
	9 Economy of Paraguay
	10 Economy of Guatemala

Case 17: search> Kraepelin's major work, &quot;Compendium der Psychiatrie&quot;, was first published in 1883.
In it, he argued that psychiatry was a branch of medical science and should be investigated by observation and
 experimentation like the other natural sciences.  He called for research into the physical causes of mental illness,
 and started to establish the foundations of the modern classification system for mental disorders.
	1 Probability distribution
	2 Foreign relations of Niger
	3 Mariner program
	4 Transport in Ireland
	5 Normed vector space
	6 Outline of literature
	7 Pakistan Armed Forces
	8 Organized crime
	9 Islamic eschatology
	10 Politics of Lesotho

Case 18: common words:
search> john
	1 John Woo
	2 John Rutsey
	3 John Major
	4 Pope
	5 John Lennon
	6 John Abercrombie (physician)
	7 John Radcliffe (physician)
	8 John Keats
	9 North Pole
	10 Fawlty Towers



---------------------------------------------------------------------------------
ACKNOWLEDGEMENT OF COLLABORATION
---------------------------------------------------------------------------------
This is a group project accomplished through the collaboration of Yueshan (Aubrey) Li, Benjamin Kilimnik, and Livia Gimenes.

** Originally this implementation subgroup group consists of Aubrey and Benjamin, Livia joined the group in the middle of
the project implementation under the consent from Kathi due to special circumstance of project partner dropping the class.

Thank you for your attention to our project and wish you have a good day! :)
