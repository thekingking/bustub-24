#include "storage/index/b_plus_tree.h"
#include <cstdio>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include "common/config.h"
#include "common/rid.h"
#include "fmt/core.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Find the leaf page that contains the input key
 * @return : page_id of the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalBinarySearch(const InternalPage *internal_page, const KeyType &key) -> int {
  int left = 1;
  int right = internal_page->GetSize();
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator_(key, internal_page->KeyAt(mid)) >= 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

/*
 * Find the leaf page that contains the input key
 * @return : page_id of the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafBinarySearch(const LeafPage *leaf_page, const KeyType &key) -> int {
  int left = 0;
  int right = leaf_page->GetSize();
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator_(key, leaf_page->KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  Context ctx;
  ReadPageGuard read_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = read_guard.As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;

  // Case1: If the tree is empty, return false.
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // Find the leaf page.
  header_page = nullptr;
  read_guard = bpm_->ReadPage(root_page_id);
  auto page = read_guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = read_guard.As<InternalPage>();
    auto index = InternalBinarySearch(internal_page, key);
    root_page_id = internal_page->ValueAt(index - 1);
    read_guard = bpm_->ReadPage(root_page_id);
    page = read_guard.As<BPlusTreePage>();
  }

  // Search the key in the leaf page.
  auto leaf_page = read_guard.As<LeafPage>();
  auto index = LeafBinarySearch(leaf_page, key);

  // Case1: If the key does not exist, return false.
  if (index == leaf_page->GetSize() || comparator_(key, leaf_page->KeyAt(index)) != 0) {
    return false;
  }

  // Case2: If the key exists, return the value.
  result->push_back(static_cast<RID>(leaf_page->ValueAt(index)));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  /* Case1: If the tree is empty, create a new tree. */
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    // Create a new leaf page as the root page.
    auto leaf_page_id = bpm_->NewPage();
    auto leaf_page_guard = bpm_->WritePage(leaf_page_id);
    auto leaf_page = leaf_page_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->Insert(0, key, value);

    // Update the root page id.
    header_page->root_page_id_ = leaf_page_id;
    return true;
  }

  // Find the leaf page.
  auto page_id = header_page->root_page_id_;
  WritePageGuard write_guard = bpm_->WritePage(page_id);
  auto page = write_guard.AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // Get the internal page.
    auto internal_page = write_guard.AsMut<InternalPage>();
    if (internal_page->GetSize() < internal_page->GetMaxSize() && ctx.header_page_.has_value()) {
      ctx.header_page_->Drop();
      header_page = nullptr;
      ctx.header_page_ = std::nullopt;
    }
    while (internal_page->GetSize() < internal_page->GetMaxSize() && ctx.write_set_.size() > 1) {
      ctx.write_set_.front().Drop();
      ctx.write_set_.pop_front();
    }

    // update the first key in the internal page (only the key in the leftmost page will be updated) )
    auto index = 0;
    if (comparator_(key, internal_page->KeyAt(0)) < 0) {
      internal_page->SetKeyAt(0, key);
      ++index;
    } else {
      index = InternalBinarySearch(internal_page, key);
    }

    // Save the search path and insert the internal pageguard into write_set
    page_id = internal_page->ValueAt(index - 1);
    ctx.write_set_.push_back(std::move(write_guard));
    ctx.index_set_.push_back(index - 1);

    write_guard = bpm_->WritePage(page_id);
    page = write_guard.AsMut<BPlusTreePage>();
  }

  // search the key in the leaf page
  auto leaf_page = write_guard.AsMut<LeafPage>();
  auto index = LeafBinarySearch(leaf_page, key);

  // Case2: If the key already exists, return false.
  if (index != leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(index)) == 0) {
    return false;
  }

  // Insert the key-value pair.
  leaf_page->Insert(index, key, value);

  // Case3: Insert successfully and the leaf page has enough space, return immediately.
  if (leaf_page->GetSize() <= leaf_page->GetMaxSize()) {
    return true;
  }

  // Case4: Insert successfully but the leaf page does not have enough space, split the leaf page.
  auto new_leaf_page_id = bpm_->NewPage();
  auto new_leaf_guard = bpm_->WritePage(new_leaf_page_id);
  auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  // Move half of the key-value pairs to the new leaf page.
  auto mid = leaf_page->GetSize() / 2;
  for (int i = 0; i < leaf_page->GetSize() - mid; ++i) {
    new_leaf_page->Insert(i, leaf_page->KeyAt(mid + i), leaf_page->ValueAt(mid + i));
  }
  leaf_page->SetSize(mid);

  // Update the next page id.
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_id);

  // save the left_key and right_key, which will be used to update the parent page key
  auto left_key = leaf_page->KeyAt(0);
  auto left_page_id = page_id;
  auto right_key = new_leaf_page->KeyAt(0);
  auto right_page_id = new_leaf_page_id;

  // Update the parent page.
  while (!ctx.write_set_.empty()) {
    // Get the parent page and the index to insert the key.
    auto internal_guard = std::move(ctx.write_set_.back());
    auto internal_page = internal_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();
    index = ctx.index_set_.back();
    ctx.index_set_.pop_back();
    internal_page->Insert(index + 1, right_key, right_page_id);

    // Case6: If the internal page has enough space, return immediately.
    if (internal_page->GetSize() <= internal_page->GetMaxSize()) {
      return true;
    }

    // Create a new internal page.
    auto new_internal_page_id = bpm_->NewPage();
    auto new_internal_guard = bpm_->WritePage(new_internal_page_id);
    auto new_internal_page = new_internal_guard.AsMut<InternalPage>();
    new_internal_page->Init(internal_max_size_);
    // Move half of the key-value pairs to the new internal page.
    auto mid = internal_page->GetSize() / 2;
    for (int i = 0; i < internal_page->GetSize() - mid; ++i) {
      new_internal_page->Insert(i, internal_page->KeyAt(mid + i), internal_page->ValueAt(mid + i));
    }
    internal_page->SetSize(mid);

    // Update the left_key and right_key.
    left_key = internal_page->KeyAt(0);
    left_page_id = internal_guard.GetPageId();
    right_key = new_internal_page->KeyAt(0);
    right_page_id = new_internal_page_id;
  }

  // Case7: the root page is full, need to split the root page and update the headerpage
  auto new_root_page_id = bpm_->NewPage();
  auto internal_guard = bpm_->WritePage(new_root_page_id);
  auto internal_page = internal_guard.AsMut<InternalPage>();
  internal_page->Init(internal_max_size_);
  internal_page->Insert(0, left_key, left_page_id);
  internal_page->Insert(1, right_key, right_page_id);
  header_page->root_page_id_ = new_root_page_id;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;

  // Case1: If the tree is empty, return immediately.
  if (root_page_id == INVALID_PAGE_ID) {
    return;
  }

  // Find the leaf page.
  auto page_id = root_page_id;
  WritePageGuard write_guard = bpm_->WritePage(page_id);
  auto page = write_guard.AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // Get the internal page.
    auto internal_page = write_guard.AsMut<InternalPage>();
    if (internal_page->GetSize() > internal_page->GetMinSize() && ctx.header_page_.has_value()) {
      ctx.header_page_->Drop();
      header_page = nullptr;
      ctx.header_page_ = std::nullopt;
    }
    while (internal_page->GetSize() > internal_page->GetMinSize() && !ctx.write_set_.empty()) {
      ctx.write_set_.front().Drop();
      ctx.write_set_.pop_front();
    }

    // Find the index to insert the key.
    auto index = InternalBinarySearch(internal_page, key);

    // Get the child page.
    if (index > 1) {
      page_id_t sibling_page_id = internal_page->ValueAt(index - 2);
      ctx.write_set_.push_back(bpm_->WritePage(sibling_page_id));
    } else if (index < internal_page->GetSize()) {
      page_id_t sibling_page_id = internal_page->ValueAt(index);
      ctx.write_set_.push_back(bpm_->WritePage(sibling_page_id));
    }
    page_id = internal_page->ValueAt(index - 1);
    ctx.write_set_.push_back(std::move(write_guard));
    ctx.index_set_.push_back(index - 1);
    write_guard = bpm_->WritePage(page_id);
    page = write_guard.AsMut<BPlusTreePage>();
  }

  // Get the leaf page and search the key.
  auto leaf_page = write_guard.AsMut<LeafPage>();
  auto index = LeafBinarySearch(leaf_page, key);

  // Case2: If the key does not exist, return immediately.
  if (index == leaf_page->GetSize() || comparator_(key, leaf_page->KeyAt(index)) != 0) {
    return;
  }

  // the remove key is internal key, need to update the internal key, which is not the root page
  leaf_page->Remove(index);

  // Case3: If the leaf page has enough space, return immediately.
  if (page_id == root_page_id) {
    if (leaf_page->GetSize() == 0) {
      header_page->root_page_id_ = INVALID_PAGE_ID;
      // bpm_->DeletePage(page_id);
    }
    return;
  }

  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    return;
  }

  auto internal_guard = std::move(ctx.write_set_.back());
  auto internal_page = internal_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  index = ctx.index_set_.back();
  ctx.index_set_.pop_back();

  // If the leaf page does not have enough space, redistribute or merge the leaf page.
  if (index > 0) {
    auto left_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto left_page = left_guard.AsMut<LeafPage>();

    // If the left page has enough space, redistribute the key.
    if (left_page->GetSize() + leaf_page->GetSize() > left_page->GetMaxSize()) {
      auto mid = (left_page->GetSize() + leaf_page->GetSize()) / 2;
      for (int i = 0; i < left_page->GetSize() - mid; ++i) {
        leaf_page->Insert(i, left_page->KeyAt(mid + i), left_page->ValueAt(mid + i));
      }
      left_page->SetSize(mid);
      internal_page->SetKeyAt(index, leaf_page->KeyAt(0));
    } else {
      // If the left page does not have enough space, merge the leaf page.
      for (int i = 0; i < leaf_page->GetSize(); ++i) {
        left_page->Insert(left_page->GetSize(), leaf_page->KeyAt(i), leaf_page->ValueAt(i));
      }
      left_page->SetNextPageId(leaf_page->GetNextPageId());
      internal_page->Remove(index);
    }
  } else if (internal_page->GetSize() > 1) {
    auto right_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto right_page = right_guard.AsMut<LeafPage>();

    // If the right page has enough space, redistribute the key.
    if (right_page->GetSize() + leaf_page->GetSize() > right_page->GetMaxSize()) {
      auto mid = (right_page->GetSize() + leaf_page->GetSize()) / 2;
      for (int i = 0; i < right_page->GetSize() - mid; ++i) {
        leaf_page->Insert(leaf_page->GetSize(), right_page->KeyAt(0), right_page->ValueAt(0));
        right_page->Remove(0);
      }
      internal_page->SetKeyAt(index + 1, right_page->KeyAt(0));
    } else {
      // If the right page does not have enough space, merge the leaf page.
      for (int i = 0; i < right_page->GetSize(); ++i) {
        leaf_page->Insert(leaf_page->GetSize(), right_page->KeyAt(i), right_page->ValueAt(i));
      }
      leaf_page->SetNextPageId(right_page->GetNextPageId());
      internal_page->Remove(index + 1);
    }
  }

  // If the internal page does not have enough space, redistribute or merge the internal page.
  while (internal_page->GetSize() < internal_page->GetMinSize() && !ctx.write_set_.empty()) {
    auto child_guard = std::move(internal_guard);
    auto child_page = internal_page;
    // Get the parent page.
    internal_guard = std::move(ctx.write_set_.back());
    internal_page = internal_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();

    // Get the index and insert the key.
    index = ctx.index_set_.back();
    ctx.index_set_.pop_back();

    if (index > 0) {
      auto left_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      auto left_page = left_guard.AsMut<InternalPage>();

      // If the left page has enough space, redistribute the key.
      if (left_page->GetSize() + child_page->GetSize() > left_page->GetMaxSize()) {
        auto mid = (left_page->GetSize() + child_page->GetSize()) / 2;
        for (int i = 0; i < left_page->GetSize() - mid; ++i) {
          child_page->Insert(i, left_page->KeyAt(mid + i), left_page->ValueAt(mid + i));
        }
        left_page->SetSize(mid);
        internal_page->SetKeyAt(index, child_page->KeyAt(0));
      } else {
        // If the left page does not have enough space, merge the leaf page.
        for (int i = 0; i < child_page->GetSize(); ++i) {
          left_page->Insert(left_page->GetSize(), child_page->KeyAt(i), child_page->ValueAt(i));
        }
        internal_page->Remove(index);
      }
    } else if (internal_page->GetSize() > 1) {
      auto right_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      auto right_page = right_guard.AsMut<InternalPage>();

      // If the right page has enough space, redistribute the key.
      if (right_page->GetSize() + child_page->GetSize() > child_page->GetMaxSize()) {
        auto mid = (right_page->GetSize() + child_page->GetSize()) / 2;
        for (int i = 0; i < right_page->GetSize() - mid; ++i) {
          child_page->Insert(child_page->GetSize(), right_page->KeyAt(0), right_page->ValueAt(0));
          right_page->Remove(0);
        }
        internal_page->SetKeyAt(index + 1, right_page->KeyAt(0));
      } else {
        // If the right page does not have enough space, merge the leaf page.
        for (int i = 0; i < right_page->GetSize(); ++i) {
          child_page->Insert(child_page->GetSize(), right_page->KeyAt(i), right_page->ValueAt(i));
        }
        internal_page->Remove(index + 1);
      }
    }
  }

  // Case5: If the root page is empty, delete the root page.
  if (internal_guard.GetPageId() == root_page_id && internal_page->GetSize() == 1) {
    header_page->root_page_id_ = internal_page->ValueAt(0);
    // bpm_->DeletePage(parent_guard.GetPageId());
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto page_id = GetRootPageId();
  if (page_id == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard read_guard = bpm_->ReadPage(page_id);
  auto page = read_guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = read_guard.As<InternalPage>();
    page_id = internal_page->ValueAt(0);
    read_guard = bpm_->ReadPage(page_id);
    page = read_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(page_id, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto page_id = GetRootPageId();
  if (page_id == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard read_guard = bpm_->ReadPage(page_id);
  auto page = read_guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = read_guard.As<InternalPage>();
    auto index = 1;
    while (index < internal_page->GetSize() && comparator_(key, internal_page->KeyAt(index)) >= 0) {
      ++index;
    }
    page_id = internal_page->ValueAt(index - 1);
    read_guard = bpm_->ReadPage(page_id);
    page = read_guard.As<BPlusTreePage>();
  }
  auto leaf_page = read_guard.As<LeafPage>();
  auto index = 0;
  while (index < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(index)) != 0) {
    ++index;
  }
  if (index == leaf_page->GetSize()) {
    return End();
  }
  return INDEXITERATOR_TYPE(page_id, index, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
