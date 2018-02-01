# Hyperlink-Induced-Topic-Search-HITS-over-Wikipedia-Articles-using-Apache-Spark

Colorado State University. Course - CS535 (Big Data). Programming Assignment - 1

This system generates a subset of graphs using Hyperlink-Induced Topic Search (HITS) over currently available Wikipedia articles.


HITS is a link analysis algorithm designed to find the key pages for specific web communities.It is also known as hubs and authorities.
HITS relies on two values associated with each page: one is called its hub score and the
other its authority score. For any query, the system computes two ranked lists of results. The ranking of
one list is induced by the hub scores and that of the other by the authority scores. 

This approach stems from a particular insight into the very early creation of web pages --- that there are two
primary kinds of web pages that are useful as results for broad-topic searches. For example,
consider an information query such as “I wish to learn about the eclipse”. There are authoritative
sources of information on the topic; in this case, the National Aeronautics and Space
Administration (NASA)’s page on eclipse would be such a page. We will call such pages as
authorities.
In contrast, there are pages containing lists of links (hand-compiled) to authoritative web
pages on a particular topic. These hub pages are not in themselves authoritative sources of
topic but they provide aggregated information. We use these hub pages to discover the
authority pages.

To compute hub and authority scores, HITS requires
following steps. I have considered only internal-links that include only the links between
Wikipedia pages.
Step 1. Given a query (e.g. “eclipse”, there are more than 100 Wikipedia pages with title
containing the text “eclipse”), get all pages containing “your-topic” in their title. Call this the
root set of pages.

Step 2. Build a base set of pages, to include the root set as well as any page that either links to a
page in the root set, or is linked to by a page in the root set.

Step 3. We then use the base set for computing hub and authority scores. To calculate the hub
and authority scores, the initial hub and authority scores of pages in the base set should be set
as 1 before the iterations. The iterations should be continued automatically until the values
converge. We should also normalize the set of values after every iteration. The authority values
should be divided by the sum of all authority values after each step. Similarly, the hub values
should be divided by the sum of all hub values after each step.

At the end, for every search query, we list the top 50 pages based on the hub score and the top 50 pages based on the authority score.



Dataset : Wiki dump data (https://wayback.archive.org/web/20160516004046/http://haselgrove.id.au/wikipedia.htm)
