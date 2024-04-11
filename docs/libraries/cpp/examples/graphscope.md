# Integrate into GraphScope

[GraphScope](https://graphscope.io/) is a unified distributed graph
computing platform that provides a one-stop environment for performing
diverse graph operations on a cluster through a user-friendly Python
interface. As an important application case of GraphAr, we have
integrated it into GraphScope.

GraphScope works on a graph G fragmented via a partition strategy picked
by the user and each worker maintains a fragment of G. Given a query, it
posts the same query to all the workers and computes following the BSP
(Bulk Synchronous Parallel) model. More specifically, each worker first
executes processing against its local fragment, to compute partial
answers in parallel. And then each worker may exchange partial results
with other processors via synchronous message passing.

To integrate GraphAr into GraphScope, we implemented
*ArrowFragmentBuilder* and *ArrowFragmentWriter*. *ArrowFragmentBuilder*
establishes the fragments for workers of GraphScope through reading GAR
files in parallel. Conversely, *ArrowFragmentWriter* can take the
GraphScope fragments and save them as GAR files. If you're interested in
knowing more about the implementation, please refer to the [source
code](https://github.com/v6d-io/v6d/commit/0eda2067e45fbb4ac46892398af0edc84fe1c27b).

## Performance Report

### Parameter settings

The time performance of *ArrowFragmentBuilder* and *ArrowFragmentWriter*
in GraphScope is heavily dependent on the partitioning of the graph into
GAR files, that is, the *vertex chunk size* and *edge chunk size*, which
are specified in the vertex information file and in the edge information
file, respectively. See [GraphAr File
Format](../user-guide/file-format.html) to understand the chunk size
definitions in GAR.

Generally speaking, fewer chunks are created if the file size is large.
On small graphs, this can be disadvantageous as it reduces the degree of
parallelism, prolonging disk I/O time. On the other hand, having too
many small files increases the overhead associated with the file system
and the file parser.

We have conducted micro benchmarks to compare the time performance for
reading/writing GAR files by
*ArrowFragmentBuilder*/*ArrowFragmentWriter*, across different *vertex
chunk size* and *edge chunk size* configurations. The settings we
recommend for *vertex chunk size* and *edge chunk size* are **2^18** and
**2^22**, respectively, which lead to efficient performance in most
cases. These settings can be used as the reference values when
integrating GraphAr into other systems besides GraphScope.

### Time performance results

Here we report the performance results of *ArrowFragmentBuilder*, and
compare it with loading the same graph through the default loading
strategy of GraphScope (through reading the csv files in parallel) . The
execution time reported below includes loading the graph data from the
disk into memory, as well as building GraphScope fragments from such
data. The experiments are conducted on a cluster of 4 AliCloud
ecs.r6.6xlarge instances (24vCPU, 192GB memory), and using
[com-friendster](https://snap.stanford.edu/data/com-Friendster.html) (a
simple graph) and [ldbc-snb-30](https://ldbcouncil.org/benchmarks/snb/)
(a multi-labeled property graph) as datasets.

<table style="width:88%;">
<colgroup>
<col style="width: 23%" />
<col style="width: 13%" />
<col style="width: 25%" />
<col style="width: 25%" />
</colgroup>
<thead>
<tr class="header">
<th>Dataset</th>
<th>Workers</th>
<th>Default Loading</th>
<th>GraphAr Loading</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>com-friendster</td>
<td>4</td>
<td>282s</td>
<td><blockquote>
<p>54s</p>
</blockquote></td>
</tr>
<tr class="even">
<td>ldbc-snb-30</td>
<td>4</td>
<td>196s</td>
<td><blockquote>
<p>40s</p>
</blockquote></td>
</tr>
</tbody>
</table>
