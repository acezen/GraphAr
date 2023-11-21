/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <mpi.h>

#include <chrono>  // NOLINT
#include <iostream>
#include <vector>

#include "grin/example/config.h"
#include "grin/predefine.h"

// GRIN headers
#include "index/internal_id.h"
#include "index/order.h"
#include "partition/partition.h"
#include "partition/reference.h"
#include "partition/topology.h"
#include "property/property.h"
#include "property/topology.h"
#include "property/type.h"
#include "property/value.h"
#include "topology/adjacentlist.h"
#include "topology/edgelist.h"
#include "topology/structure.h"
#include "topology/vertexlist.h"

GRIN_GRAPH init(GRIN_PARTITIONED_GRAPH partitioned_graph, int pid = 0) {
  // get local graph
  auto partition = grin_get_partition_by_id(partitioned_graph, pid);
  auto graph = grin_get_local_graph_by_partition(partitioned_graph, partition);

  // destroy partition
  grin_destroy_partition(partitioned_graph, partition);

  std::cout << "Init GRIN_GRAPH completed for partition " << pid << std::endl;
  return graph;
}

void run_pagerank(GRIN_PARTITIONED_GRAPH graph, bool print_result = false) {
  // initialize parameters
  const double damping = 0.85;
  const int max_iters = 1;
  // get vertex list & select by vertex type
  auto vtype = grin_get_vertex_type_by_name(graph, "person");
  auto etype = grin_get_edge_type_by_name(graph, "knows");
  auto vertex_list = grin_get_vertex_list_by_type(graph, vtype);
  const size_t num_vertices = grin_get_vertex_num_by_type(graph, vtype);
  std::cout << "num_vertices = " << num_vertices << std::endl;

  // initialize MPI
  int pid = 0, is_master = 0, n_procs = 0;
  MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
  MPI_Comm_rank(MPI_COMM_WORLD, &pid);
  is_master = (pid == 0);

  std::cout << "++++ Run distributed PageRank algorithm start for partition "
            << pid << " ++++" << std::endl;

  // select master
  auto master_vertex_list =
      grin_get_vertex_list_by_type_select_master(graph, vtype);
  const int64_t num_masters =
      grin_get_vertex_list_size(graph, master_vertex_list);
  std::cout << "pid = " << pid << ", num_masters = " << num_masters
            << std::endl;
  size_t total_num_masters = 0;
  // MPI_Allreduce(&num_masters, &total_num_masters, 1, MPI_UNSIGNED_LONG, MPI_SUM,
  //               MPI_COMM_WORLD);
  // ASSERT(total_num_masters == num_vertices);

  // initialize cnt and offset for MPI_Allgatherv
  std::vector<int64_t> cnt(n_procs), offset(n_procs);
  MPI_Allgather(&num_masters, 1, MPI_LONG, cnt.data(), 1, MPI_LONG,
                MPI_COMM_WORLD);
  offset[0] = 0;
  for (int i = 1; i < n_procs; ++i) {
    offset[i] = offset[i - 1] + cnt[i - 1];
  }
  MPI_Bcast(cnt.data(), n_procs, MPI_LONG, 0, MPI_COMM_WORLD);
  MPI_Bcast(offset.data(), n_procs, MPI_LONG, 0, MPI_COMM_WORLD);

  // initialize pagerank value
  std::vector<double> pr_curr(num_vertices);
  std::vector<double> next(num_masters);
  for (auto i = 0; i < num_vertices; ++i) {
    pr_curr[i] = 1 / static_cast<double>(num_vertices);
  }

  // initialize out degree
  std::vector<size_t> out_degree(num_vertices, 0);
  for (size_t i = 0; i < num_masters; ++i) {
    // get vertex
    auto v = grin_get_vertex_from_list(graph, master_vertex_list, i);
    auto id =
        grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v);
    out_degree[id] = grin_get_out_degree(graph, etype, v);
    // get outgoing adjacent list
    /*
    auto adjacent_list = grin_get_adjacent_list_by_edge_type(
        graph, GRIN_DIRECTION::OUT, v, etype);
    auto it = grin_get_adjacent_list_begin(graph, adjacent_list);
    while (grin_is_adjacent_list_end(graph, it) == false) {
      out_degree[id]++;
      grin_get_next_adjacent_list_iter(graph, it);
    }
    // destroy
    grin_destroy_adjacent_list_iter(graph, it);
    grin_destroy_adjacent_list(graph, adjacent_list);
    */
    grin_destroy_vertex(graph, v);
  }
  // synchronize out degree
  MPI_Allreduce(MPI_IN_PLACE, out_degree.data(), num_vertices,
                MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

  // run pagerank algorithm for #max_iters iterators
  for (int iter = 0; iter < max_iters; iter++) {
    std::cout << "pid = " << pid << ", iter = " << iter << std::endl;

    auto iter_start = std::chrono::high_resolution_clock::now();  // run start
    // update pagerank value
    // for (auto i = 0; i < num_masters; ++i) {
      // get vertex
    auto v = grin_get_vertex_from_list(graph, master_vertex_list, 0);
    auto last_v = grin_get_vertex_from_list(graph, master_vertex_list, num_masters - 1);
    auto external_id = grin_get_vertex_external_id_of_int64(graph, last_v);
      // get incoming adjacent list
    auto adjacent_list = grin_get_adjacent_list_by_edge_type(
        graph, GRIN_DIRECTION::IN, v, etype);
    auto it = grin_get_adjacent_list_begin(graph, adjacent_list);
      // update pagerank value
    next[i] = 0;
    while (grin_is_adjacent_list_end(graph, it, external_id) == false) {
      // get neighbor
      auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it);
      auto nbr_id = grin_get_position_of_vertex_from_sorted_list(
          graph, vertex_list, nbr);
      next[i] += pr_curr[nbr_id] / out_degree[nbr_id];
      grin_destroy_vertex(graph, nbr);
      grin_get_next_adjacent_list_iter(graph, it);
    }
      // destroy
    grin_destroy_adjacent_list_iter(graph, it);
    grin_destroy_adjacent_list(graph, adjacent_list);
    grin_destroy_vertex(graph, v);
    grin_destroy_vertex(graph, last_v);
    auto iter_end = std::chrono::high_resolution_clock::now();  // run start
    auto iter_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      iter_end - iter_start);

    std::cout << "Run time for iteration " << iter << " for pid " << pid << " = "
              << iter_time.count() << " ms" << std::endl;
    MPI_Barrier(MPI_COMM_WORLD);
    // apply updated values
    for (auto i = 0; i < num_masters; ++i) {
      // get vertex
      auto v = grin_get_vertex_from_list(graph, master_vertex_list, i);
      auto id =
          grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v);
      next[i] = damping * next[i] +
                (1 - damping) * (1 / static_cast<double>(num_vertices));
      if (out_degree[id] == 0)
        next[i] += damping * pr_curr[id];
      grin_destroy_vertex(graph, v);
    }

    // synchronize pagerank value
    // MPI_Allgatherv(next.data(), num_masters, MPI_DOUBLE, pr_curr.data(),
    //                cnt.data(), offset.data(), MPI_DOUBLE, MPI_COMM_WORLD);
  }

  // output results
  if (is_master && print_result) {
    auto type = grin_get_vertex_type_by_name(graph, "person");
    auto property = grin_get_vertex_property_by_name(graph, type, "id");
    auto data_type = grin_get_vertex_property_datatype(graph, property);

    for (size_t i = 0; i < num_vertices; i++) {
      // get vertex
      auto v = grin_get_vertex_from_list(graph, vertex_list, i);
      // output
      std::cout << "vertex " << i;
      if (data_type == GRIN_DATATYPE::Int64) {
        // get property "id" of vertex
        auto value =
            grin_get_vertex_property_value_of_int64(graph, v, property);
        std::cout << ", id = " << value;
      }
      std::cout << ", pagerank value = " << pr_curr[i] << std::endl;
      // destroy vertex
      grin_destroy_vertex(graph, v);
    }

    // destroy
    grin_destroy_vertex_property(graph, property);
    grin_destroy_vertex_type(graph, type);
  }

  grin_destroy_vertex_list(graph, master_vertex_list);
  grin_destroy_vertex_list(graph, vertex_list);
  grin_destroy_vertex_type(graph, vtype);
  grin_destroy_edge_type(graph, etype);

  std::cout
      << "---- Run distributed PageRank algorithm completed for partition "
      << pid << " -----" << std::endl;
}

int main(int argc, char* argv[]) {
  auto init_start = std::chrono::high_resolution_clock::now();  // init start

  // MPI initialize
  int flag = 0, pid = 0, n_procs = 0, is_master = 0;
  MPI_Initialized(&flag);
  if (!flag)
    MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &pid);
  MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
  is_master = (pid == 0);

  // set graph info path and partition number
  std::string path = "/apsara/weibin/graphar/cf_21/cf.graph/yml";
  // set partition number = n_procs, stragey = segmented
  uint32_t partition_num = n_procs;
  if (is_master) {
    std::cout << "GraphInfo path = " << path << std::endl;
    std::cout << "Partition strategy = segmented" << std::endl;
    std::cout << "Partition number = " << partition_num << std::endl;
  }

  // get partitioned graph from graph info of GraphAr
  std::string partitioned_path =
      "graphar://" + path + "?partition_num=" + std::to_string(partition_num) +
      "&strategy=" + "segmented";
  if (is_master) {
    std::cout << "graph uri = " << partitioned_path << std::endl;
  }
  char* uri = new char[partitioned_path.length() + 1];
  snprintf(uri, partitioned_path.length() + 1, "%s", partitioned_path.c_str());
  GRIN_PARTITIONED_GRAPH pg = grin_get_partitioned_graph_from_storage(uri);
  delete[] uri;
  // get local graph from partitioned graph
  GRIN_GRAPH graph = init(pg, pid);

  auto init_end = std::chrono::high_resolution_clock::now();  // init end
  auto init_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      init_end - init_start);

  // run pagerank algorithm
  auto run_start = std::chrono::high_resolution_clock::now();  // run start
  run_pagerank(graph);  // do not print result by default
  auto run_end = std::chrono::high_resolution_clock::now();  // run end
  auto run_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      run_end - run_start);

  // destroy
  grin_destroy_graph(graph);
  grin_destroy_partitioned_graph(pg);

  // MPI finalize
  MPI_Finalize();

  // output execution time
  if (is_master) {
    std::cout << "Init time for distributed PageRank with GRIN = "
              << init_time.count() << " ms" << std::endl;
    std::cout << "Run time for distributed PageRank with GRIN = "
              << run_time.count() << " ms" << std::endl;
    std::cout << "Total time for distributed PageRank with GRIN = "
              << init_time.count() + run_time.count() << " ms" << std::endl;
  }

  return 0;
}
