#include <iostream>

#include "arrow/api.h"
#include "arrow/filesystem/api.h"

#include "./config.h"
#include "gar/api.h"
#include "gar/writer/edges_builder.h"
#include "gar/writer/vertices_builder.h"

void vertices_builder() {
    // construct vertices builder
    std::string vertex_meta_file = "/Users/bryaneno/Work/graphar/GraphAr/build/person.vertex.yml";
    auto vertex_meta = graphar::Yaml::LoadFile(vertex_meta_file).value();
    auto vertex_info = graphar::VertexInfo::Load(vertex_meta).value();
    graphar::IdType start_index = 0;
    graphar::builder::VerticesBuilder builder(vertex_info, "/Users/bryaneno/Work/graphar/GraphAr/build/generate-temp/", start_index);

    // set validate level
    builder.SetValidateLevel(graphar::ValidateLevel::strong_validate);

    // prepare vertex data
    int vertex_count = 3;
    std::vector<std::string> property_names = {"id", "firstName", "lastName",
                                               "gender"};
    std::vector<int64_t> id = {0, 1, 2};
    std::vector<std::string> firstName = {"John", "Jane", "Alice"};
    std::vector<std::string> lastName = {"Smith", "Doe", "Wonderland"};
    std::vector<std::string> gender = {"male", "famale", "famale"};

    // add vertices
    for (int i = 0; i < vertex_count; i++) {
        graphar::builder::Vertex v;
        v.AddProperty(property_names[0], id[i]);
        v.AddProperty(property_names[1], firstName[i]);
        v.AddProperty(property_names[2], lastName[i]);
        v.AddProperty(property_names[3], gender[i]);
        ASSERT(builder.AddVertex(v).ok());
    }

    // dump
    ASSERT(builder.GetNum() == vertex_count);
    std::cout << "vertex_count=" << builder.GetNum() << std::endl;
    ASSERT(builder.Dump().ok());
    std::cout << "dump vertices collection successfully!" << std::endl;

    // clear vertices
    builder.Clear();
    ASSERT(builder.GetNum() == 0);
}

void edges_builder() {
    // construct edges builder
    std::string edge_meta_file = "/Users/bryaneno/Work/graphar/GraphAr/build/person_knows_person.edge.yml";
    auto edge_meta = graphar::Yaml::LoadFile(edge_meta_file).value();
    auto edge_info = graphar::EdgeInfo::Load(edge_meta).value();
    auto vertex_count = 3;
    graphar::builder::EdgesBuilder builder(
            edge_info, "/Users/bryaneno/Work/graphar/GraphAr/build/generate-temp/", graphar::AdjListType::ordered_by_dest, vertex_count);

    // set validate level
    builder.SetValidateLevel(graphar::ValidateLevel::strong_validate);

    // prepare edge data
    int edge_count = 4;
    std::vector<std::string> property_names = {"creationDate"};
    std::vector<int64_t> src = {1, 0, 0, 2};
    std::vector<int64_t> dst = {0, 1, 2, 1};
    std::vector<std::string> creationDate = {"2010-01-01", "2011-01-01",
                                             "2012-01-01", "2013-01-01"};

    // add edges
    for (int i = 0; i < edge_count; i++) {
        graphar::builder::Edge e(src[i], dst[i]);
        e.AddProperty("creationDate", creationDate[i]);
        ASSERT(builder.AddEdge(e).ok());
    }

    // dump
    ASSERT(builder.GetNum() == edge_count);
    std::cout << "edge_count=" << builder.GetNum() << std::endl;
    ASSERT(builder.Dump().ok());
    std::cout << "dump edges collection successfully!" << std::endl;

    // clear edges
    builder.Clear();
    ASSERT(builder.GetNum() == 0);
}

int main(int argc, char* argv[]) {
    // vertices builder
    std::cout << "Vertices builder" << std::endl;
    std::cout << "-------------------" << std::endl;
    vertices_builder();
    std::cout << std::endl;

    // edges builder
    std::cout << "Edges builder" << std::endl;
    std::cout << "----------------" << std::endl;
    edges_builder();
}