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

#include "gar/graph.h"
#include "gar/util/convert_to_arrow_type.h"

namespace GAR_NAMESPACE_INTERNAL {

template <Type type>
Status CastToAny(std::shared_ptr<arrow::Array> array,
                 std::any& any) {  // NOLINT
  using ArrayType = typename ConvertToArrowType<type>::ArrayType;
  auto column = std::dynamic_pointer_cast<ArrayType>(array);
  any = column->GetView(0);
  return Status::OK();
}

template <>
Status CastToAny<Type::STRING>(std::shared_ptr<arrow::Array> array,
                               std::any& any) {  // NOLINT
  using ArrayType = typename ConvertToArrowType<Type::STRING>::ArrayType;
  auto column = std::dynamic_pointer_cast<ArrayType>(array);
  any = column->GetString(0);
  return Status::OK();
}

Status TryToCastToAny(const DataType& type, std::shared_ptr<arrow::Array> array,
                      std::any& any) {  // NOLINT
  switch (type.id()) {
  case Type::BOOL:
    return CastToAny<Type::BOOL>(array, any);
  case Type::INT32:
    return CastToAny<Type::INT32>(array, any);
  case Type::INT64:
    return CastToAny<Type::INT64>(array, any);
  case Type::FLOAT:
    return CastToAny<Type::FLOAT>(array, any);
  case Type::DOUBLE:
    return CastToAny<Type::DOUBLE>(array, any);
  case Type::STRING:
    return CastToAny<Type::STRING>(array, any);
  default:
    return Status::TypeError("Unsupported type.");
  }
  return Status::OK();
}

Vertex::Vertex(IdType id,
               std::vector<VertexPropertyArrowChunkReader>& readers)  // NOLINT
    : id_(id) {
  // get the first row of table
  for (auto& reader : readers) {
    GAR_ASSIGN_OR_RAISE_ERROR(auto chunk_table, reader.GetChunk());
    auto schema = chunk_table->schema();
    for (int i = 0; i < schema->num_fields(); ++i) {
      auto field = chunk_table->field(i);
      auto type = DataType::ArrowDataTypeToDataType(field->type());
      GAR_RAISE_ERROR_NOT_OK(TryToCastToAny(
          type, chunk_table->column(i)->chunk(0), properties_[field->name()]));
    }
  }
}

Edge::Edge(
    std::shared_ptr<AdjListArrowChunkReader>& adj_list_reader,                          // NOLINT
    std::vector<AdjListPropertyArrowChunkReader>& property_readers) {  // NOLINT
  // get the first row of table
  GAR_ASSIGN_OR_RAISE_ERROR(auto adj_list_chunk_table,
                            adj_list_reader->GetChunk());
  src_id_ = std::dynamic_pointer_cast<arrow::Int64Array>(
                adj_list_chunk_table->column(0)->chunk(0))
                ->GetView(0);
  dst_id_ = std::dynamic_pointer_cast<arrow::Int64Array>(
                adj_list_chunk_table->column(1)->chunk(0))
                ->GetView(0);
  for (auto& reader : property_readers) {
    // get the first row of table
    GAR_ASSIGN_OR_RAISE_ERROR(auto chunk_table, reader.GetChunk());
    auto schema = chunk_table->schema();
    for (int i = 0; i < schema->num_fields(); ++i) {
      auto field = chunk_table->field(i);
      auto type = DataType::ArrowDataTypeToDataType(field->type());
      GAR_RAISE_ERROR_NOT_OK(TryToCastToAny(
          type, chunk_table->column(i)->chunk(0), properties_[field->name()]));
    }
  }
}

IdType EdgeIter::source() {
  adj_list_reader_->seek(cur_offset_);
  GAR_ASSIGN_OR_RAISE_ERROR(auto chunk, adj_list_reader_->GetChunk());
  auto src_column = chunk->column(0);
  return std::dynamic_pointer_cast<arrow::Int64Array>(src_column->chunk(0))
      ->GetView(0);
}

IdType EdgeIter::destination() {
  adj_list_reader_->seek(cur_offset_);
  GAR_ASSIGN_OR_RAISE_ERROR(auto chunk, adj_list_reader_->GetChunk());
  auto src_column = chunk->column(1);
  return std::dynamic_pointer_cast<arrow::Int64Array>(src_column->chunk(0))
      ->GetView(0);
}

bool EdgeIter::first_src(const EdgeIter& from, IdType id) {
  if (from.is_end())
    return false;

  // ordered_by_dest or unordered_by_dest
  if (adj_list_type_ == AdjListType::ordered_by_dest ||
      adj_list_type_ == AdjListType::unordered_by_dest) {
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      this->refresh();
    }
    while (!this->is_end()) {
      if (this->source() == id)
        return true;
      this->operator++();
    }
    return false;
  }

  // unordered_by_source
  if (adj_list_type_ == AdjListType::unordered_by_source) {
    IdType expect_chunk_index =
        index_converter_->IndexPairToGlobalChunkIndex(id / src_chunk_size_, 0);
    if (expect_chunk_index > chunk_end_)
      return false;
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    bool need_refresh = false;
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      need_refresh = true;
    }
    if (global_chunk_index_ < expect_chunk_index) {
      global_chunk_index_ = expect_chunk_index;
      cur_offset_ = 0;
      vertex_chunk_index_ = id / src_chunk_size_;
      need_refresh = true;
    }
    if (need_refresh)
      this->refresh();
    while (!this->is_end()) {
      if (this->source() == id)
        return true;
      if (vertex_chunk_index_ > id / src_chunk_size_)
        return false;
      this->operator++();
    }
    return false;
  }

  // ordered_by_source
  auto st = offset_reader_->seek(id);
  if (!st.ok()) {
    return false;
  }
  auto maybe_offset_chunk = offset_reader_->GetChunk();
  if (!maybe_offset_chunk.status().ok()) {
    return false;
  }
  auto offset_array =
      std::dynamic_pointer_cast<arrow::Int64Array>(maybe_offset_chunk.value());
  auto begin_offset = static_cast<IdType>(offset_array->Value(0));
  auto end_offset = static_cast<IdType>(offset_array->Value(1));
  if (begin_offset >= end_offset) {
    return false;
  }
  auto vertex_chunk_index_of_id = offset_reader_->GetChunkIndex();
  auto begin_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, begin_offset / chunk_size_);
  auto end_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, end_offset / chunk_size_);
  if (begin_global_index <= from.global_chunk_index_ &&
      from.global_chunk_index_ <= end_global_index) {
    if (begin_offset < from.cur_offset_ && from.cur_offset_ < end_offset) {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      refresh();
      return true;
    } else if (from.cur_offset_ <= begin_offset) {
      global_chunk_index_ = begin_global_index;
      cur_offset_ = begin_offset;
      vertex_chunk_index_ = vertex_chunk_index_of_id;
      refresh();
      return true;
    } else {
      return false;
    }
  } else if (from.global_chunk_index_ < begin_global_index) {
    global_chunk_index_ = begin_global_index;
    cur_offset_ = begin_offset;
    vertex_chunk_index_ = vertex_chunk_index_of_id;
    refresh();
    return true;
  } else {
    return false;
  }
}

bool EdgeIter::first_dst(const EdgeIter& from, IdType id) {
  if (from.is_end())
    return false;

  // ordered_by_source or unordered_by_source
  if (adj_list_type_ == AdjListType::ordered_by_source ||
      adj_list_type_ == AdjListType::unordered_by_source) {
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      this->refresh();
    }
    while (!this->is_end()) {
      if (this->destination() == id)
        return true;
      this->operator++();
    }
    return false;
  }

  // unordered_by_dest
  if (adj_list_type_ == AdjListType::unordered_by_dest) {
    IdType expect_chunk_index =
        index_converter_->IndexPairToGlobalChunkIndex(id / dst_chunk_size_, 0);
    if (expect_chunk_index > chunk_end_)
      return false;
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    bool need_refresh = false;
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      need_refresh = true;
    }
    if (global_chunk_index_ < expect_chunk_index) {
      global_chunk_index_ = expect_chunk_index;
      cur_offset_ = 0;
      vertex_chunk_index_ = id / dst_chunk_size_;
      need_refresh = true;
    }
    if (need_refresh)
      this->refresh();
    while (!this->is_end()) {
      if (this->destination() == id)
        return true;
      if (vertex_chunk_index_ > id / dst_chunk_size_)
        return false;
      this->operator++();
    }
    return false;
  }

  // ordered_by_dest
  auto st = offset_reader_->seek(id);
  if (!st.ok()) {
    return false;
  }
  auto maybe_offset_chunk = offset_reader_->GetChunk();
  if (!maybe_offset_chunk.status().ok()) {
    return false;
  }
  auto offset_array =
      std::dynamic_pointer_cast<arrow::Int64Array>(maybe_offset_chunk.value());
  auto begin_offset = static_cast<IdType>(offset_array->Value(0));
  auto end_offset = static_cast<IdType>(offset_array->Value(1));
  if (begin_offset >= end_offset) {
    return false;
  }
  auto vertex_chunk_index_of_id = offset_reader_->GetChunkIndex();
  auto begin_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, begin_offset / chunk_size_);
  auto end_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, end_offset / chunk_size_);
  if (begin_global_index <= from.global_chunk_index_ &&
      from.global_chunk_index_ <= end_global_index) {
    if (begin_offset < from.cur_offset_ && from.cur_offset_ < end_offset) {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      refresh();
      return true;
    } else if (from.cur_offset_ <= begin_offset) {
      global_chunk_index_ = begin_global_index;
      cur_offset_ = begin_offset;
      vertex_chunk_index_ = vertex_chunk_index_of_id;
      refresh();
      return true;
    } else {
      return false;
    }
  } else if (from.global_chunk_index_ < begin_global_index) {
    global_chunk_index_ = begin_global_index;
    cur_offset_ = begin_offset;
    vertex_chunk_index_ = vertex_chunk_index_of_id;
    refresh();
    return true;
  } else {
    return false;
  }
}

IdType EdgesCollection<AdjListType::ordered_by_source>::count_src(IdType id) {
  offset_reader_->seek(id);
  auto array = std::static_pointer_cast<arrow::Int64Array>(offset_reader_->GetChunk().value());
  return array->Value(1) - array->Value(0);
}

const AdjListType
    EdgesCollection<AdjListType::ordered_by_source>::adj_list_type_ =
        AdjListType::ordered_by_source;
const AdjListType
    EdgesCollection<AdjListType::ordered_by_dest>::adj_list_type_ =
        AdjListType::ordered_by_dest;
const AdjListType
    EdgesCollection<AdjListType::unordered_by_source>::adj_list_type_ =
        AdjListType::unordered_by_source;
const AdjListType
    EdgesCollection<AdjListType::unordered_by_dest>::adj_list_type_ =
        AdjListType::unordered_by_dest;

}  // namespace GAR_NAMESPACE_INTERNAL
