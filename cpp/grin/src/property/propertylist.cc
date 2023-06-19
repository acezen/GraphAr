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

#include "grin/src/predefine.h"
// GRIN headers
#include "property/propertylist.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_property_list_by_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (vtype >= _g->vertex_type_num)
    return GRIN_NULL_VERTEX_PROPERTY_LIST;
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  for (unsigned i = _g->vertex_property_offsets[vtype];
       i < _g->vertex_property_offsets[vtype + 1]; ++i) {
    vpl->push_back(i);
  }
  return vpl;
}

size_t grin_get_vertex_property_list_size(GRIN_GRAPH g,
                                          GRIN_VERTEX_PROPERTY_LIST vpl) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  return _vpl->size();
}

GRIN_VERTEX_PROPERTY grin_get_vertex_property_from_list(
    GRIN_GRAPH g, GRIN_VERTEX_PROPERTY_LIST vpl, size_t idx) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  return (*_vpl)[idx];
}

GRIN_VERTEX_PROPERTY_LIST grin_create_vertex_property_list(GRIN_GRAPH g) {
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  return vpl;
}

void grin_destroy_vertex_property_list(GRIN_GRAPH g,
                                       GRIN_VERTEX_PROPERTY_LIST vpl) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  delete _vpl;
}

bool grin_insert_vertex_property_to_list(GRIN_GRAPH g,
                                         GRIN_VERTEX_PROPERTY_LIST vpl,
                                         GRIN_VERTEX_PROPERTY vp) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  _vpl->push_back(vp);
  return true;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_id(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vt, GRIN_VERTEX_PROPERTY_ID vpi) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (vt >= _g->vertex_type_num)
    return GRIN_NULL_VERTEX_PROPERTY;
  auto vp = vpi + _g->vertex_property_offsets[vt];
  if (vp < _g->vertex_property_offsets[vt + 1])
    return vp;
  else
    return GRIN_NULL_VERTEX_PROPERTY;
}

GRIN_VERTEX_PROPERTY_ID grin_get_vertex_property_id(GRIN_GRAPH g,
                                                    GRIN_VERTEX_TYPE vt,
                                                    GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (vt >= _g->vertex_type_num)
    return GRIN_NULL_VERTEX_PROPERTY_ID;
  if (vp >= _g->vertex_property_offsets[vt] &&
      vp < _g->vertex_property_offsets[vt + 1])
    return vp - _g->vertex_property_offsets[vt];
  else
    return GRIN_NULL_VERTEX_PROPERTY_ID;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
GRIN_EDGE_PROPERTY_LIST grin_get_edge_property_list_by_type(
    GRIN_GRAPH g, GRIN_EDGE_TYPE etype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_EDGE_PROPERTY_LIST;
  auto epl = new GRIN_EDGE_PROPERTY_LIST_T();
  for (unsigned i = _g->edge_property_offsets[etype];
       i < _g->edge_property_offsets[etype + 1]; ++i) {
    epl->push_back(i);
  }
  return epl;
}

size_t grin_get_edge_property_list_size(GRIN_GRAPH g,
                                        GRIN_EDGE_PROPERTY_LIST epl) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  return _epl->size();
}

GRIN_EDGE_PROPERTY grin_get_edge_property_from_list(GRIN_GRAPH g,
                                                    GRIN_EDGE_PROPERTY_LIST epl,
                                                    size_t idx) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  return (*_epl)[idx];
}

GRIN_EDGE_PROPERTY_LIST grin_create_edge_property_list(GRIN_GRAPH g) {
  auto epl = new GRIN_EDGE_PROPERTY_LIST_T();
  return epl;
}

void grin_destroy_edge_property_list(GRIN_GRAPH g,
                                     GRIN_EDGE_PROPERTY_LIST epl) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  delete _epl;
}

bool grin_insert_edge_property_to_list(GRIN_GRAPH g,
                                       GRIN_EDGE_PROPERTY_LIST epl,
                                       GRIN_EDGE_PROPERTY ep) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  _epl->push_back(ep);
  return true;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
GRIN_EDGE_PROPERTY grin_get_edge_property_by_id(GRIN_GRAPH g, GRIN_EDGE_TYPE et,
                                                GRIN_EDGE_PROPERTY_ID epi) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (et >= _g->unique_edge_type_num)
    return GRIN_NULL_EDGE_PROPERTY;
  auto ep = epi + _g->edge_property_offsets[et];
  if (ep < _g->edge_property_offsets[et + 1])
    return ep;
  else
    return GRIN_NULL_EDGE_PROPERTY;
}

GRIN_EDGE_PROPERTY_ID grin_get_edge_property_id(GRIN_GRAPH g, GRIN_EDGE_TYPE et,
                                                GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (et >= _g->unique_edge_type_num)
    return GRIN_NULL_EDGE_PROPERTY_ID;
  if (ep >= _g->edge_property_offsets[et] &&
      ep < _g->edge_property_offsets[et + 1])
    return ep - _g->edge_property_offsets[et];
  else
    return GRIN_NULL_EDGE_PROPERTY_ID;
}
#endif
