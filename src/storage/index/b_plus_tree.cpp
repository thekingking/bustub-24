#include "storage/index/b_plus_tree.h"
#include "common/config.h"
#include "common/rid.h"
#include "fmt/core.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
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
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // fmt::println(stderr, "Search key: {}", key.ToString());
  Context ctx;
  (void)ctx;
  ReadPageGuard read_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = read_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    fmt::println(stderr, "The tree is empty.");
    return false;
  }
  while (true) {
    read_guard = bpm_->ReadPage(root_page_id);
    auto page = read_guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto internal_page = read_guard.As<InternalPage>();
    auto index = 1;
    while (index < internal_page->GetSize() && comparator_(key, internal_page->KeyAt(index)) >= 0) {
      ++index;
    }
    root_page_id = internal_page->ValueAt(index - 1);
  }
  auto leaf_page = read_guard.As<LeafPage>();
  auto index = 0;
  while (index < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(index)) != 0) {
    ++index;
  }
  if (index == leaf_page->GetSize()) {
    fmt::println(stderr, "The key does not exist.");
    return false;
  }
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
  // fmt::println(stderr, "Insert key: {}, value: {}", key.ToString(), value.ToString());
  // Declaration of context instance.
  Context ctx;
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;

  // Case1: If the tree is empty, create a new tree.
  if (root_page_id == INVALID_PAGE_ID) {
    // Create a new leaf page as the root page.
    auto leaf_page_id = bpm_->NewPage();
    auto leaf_page_guard = bpm_->WritePage(leaf_page_id);
    auto leaf_page = leaf_page_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->Insert(0, key, value);

    // Update the root page id.
    root_page_id = leaf_page_id;
    header_page->root_page_id_ = root_page_id;

    return true;
  }

  // Case2: If the tree is not empty, insert into the leaf page.
  // Find the leaf page.
  auto internal_page_id = root_page_id;
  while (true) {
    auto write_guard = bpm_->WritePage(internal_page_id);
    auto page = write_guard.AsMut<BPlusTreePage>();
    if (page->IsLeafPage()) {
      ctx.write_set_.push_back(std::move(write_guard));
      break;
    }
    auto internal_page = write_guard.AsMut<InternalPage>();
    ctx.write_set_.push_back(std::move(write_guard));

    // Find the index to insert the key.
    auto index = 1;
    while (index < internal_page->GetSize() && comparator_(key, internal_page->KeyAt(index)) > 0) {
      ++index;
    }

    // Case2.1: If the key already exists, return false.
    if (index < internal_page->GetSize() && comparator_(key, internal_page->KeyAt(index)) == 0) {
      return false;
    }

    ctx.index_set_.push_back(index - 1);
    internal_page_id = internal_page->ValueAt(index - 1);
  }

  // Get the leaf page.
  auto leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf_page = leaf_page_guard.AsMut<LeafPage>();

  auto index = 0;
  while (index < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(index)) > 0) {
    ++index;
  }
  // Case2.1: If the key already exists, return false.
  if (index != leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(index)) == 0) {
    return false;
  }

  // Insert the key-value pair.
  leaf_page->Insert(index, key, value);

  // Case2.2: If the key does not exist and the leaf page has enough space
  if (leaf_page->GetSize() <= leaf_page->GetMaxSize()) {
    return true;
  }

  // Case2.3: If the key does not exist and the leaf page does not have enough space, split the leaf page.
  auto new_leaf_page_id = bpm_->NewPage();
  auto new_leaf_guard = bpm_->WritePage(new_leaf_page_id);
  auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  // Move half of the key-value pairs to the new leaf page.
  auto mid = leaf_page->GetSize() / 2;
  for (int i = mid; i < leaf_page->GetSize(); ++i) {
    new_leaf_page->SetKeyAt(i - mid, leaf_page->KeyAt(i));
    new_leaf_page->SetValueAt(i - mid, leaf_page->ValueAt(i));
  }
  new_leaf_page->SetSize(leaf_page->GetSize() - mid);
  leaf_page->SetSize(mid);

  // Update the next page id.
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_id);

  // Insert the new key to the parent page.
  auto left_child_page_id = leaf_page_guard.GetPageId();
  auto right_child_page_id = new_leaf_page_id;
  auto child_key = new_leaf_page->KeyAt(0);
  while (!ctx.write_set_.empty()) {
    // Get the parent page.
    auto parent_guard = std::move(ctx.write_set_.back());
    auto parent_internal_page = parent_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();

    // Find the index to insert the key.
    auto index = ctx.index_set_.back();
    ctx.index_set_.pop_back();
    parent_internal_page->Insert(index + 1, child_key, right_child_page_id);

    // Case1: If the parent page has enough space, insert the key.
    if (parent_internal_page->GetSize() <= parent_internal_page->GetMaxSize()) {
      return true;
    }

    // Case2: If the parent page does not have enough space, split the parent page.

    // Create a new internal page.
    auto new_internal_page_id = bpm_->NewPage();
    auto new_internal_guard = bpm_->WritePage(new_internal_page_id);
    auto new_internal_page = new_internal_guard.AsMut<InternalPage>();
    new_internal_page->Init(internal_max_size_);

    // Move half of the key-value pairs to the new internal page.
    auto mid = parent_internal_page->GetSize() / 2;
    new_internal_page->Insert(0, parent_internal_page->KeyAt(0), parent_internal_page->ValueAt(mid));
    for (int i = mid + 1; i < parent_internal_page->GetSize(); ++i) {
      new_internal_page->Insert(i - mid, parent_internal_page->KeyAt(i), parent_internal_page->ValueAt(i));
    }
    new_internal_page->SetSize(parent_internal_page->GetSize() - mid);
    parent_internal_page->SetSize(mid);

    // Update the child page id.
    left_child_page_id = parent_guard.GetPageId();
    right_child_page_id = new_internal_page_id;
    child_key = parent_internal_page->KeyAt(mid);
  }

  // split the root page
  auto new_root_page_id = bpm_->NewPage();
  auto new_root_guard = bpm_->WritePage(new_root_page_id);
  auto new_root_page = new_root_guard.AsMut<InternalPage>();
  new_root_page->Init(internal_max_size_);

  // Insert the key-value pairs.
  new_root_page->Insert(0, child_key, left_child_page_id);
  new_root_page->Insert(1, child_key, right_child_page_id);

  // Update the root page_id
  root_page_id = new_root_page_id;
  header_page->root_page_id_ = root_page_id;
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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
