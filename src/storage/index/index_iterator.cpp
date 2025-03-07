/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <cassert>
#include <memory>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (mutex_ != nullptr) {
    mutex_->unlock_shared();
  }
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *bpm, std::shared_ptr<std::shared_mutex> mutex)
    : page_id_(page_id), index_(index), bpm_(bpm), mutex_(mutex) {
  if (mutex_ != nullptr) {
    mutex_->lock_shared();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto read_guard = bpm_->ReadPage(page_id_);
  auto leaf_page = read_guard.As<LeafPage>();
  return std::make_pair(leaf_page->KeyAt(index_), leaf_page->ValueAt(index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  auto read_guard = bpm_->ReadPage(page_id_);
  auto leaf_page = read_guard.As<LeafPage>();
  if (index_ + 1 < leaf_page->GetSize()) {
    ++index_;
  } else {
    page_id_ = leaf_page->GetNextPageId();
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
