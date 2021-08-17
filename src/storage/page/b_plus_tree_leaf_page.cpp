//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include <algorithm>
#include <cstring>
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }


/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int low = 0, high = GetSize(), mid;
  while (low < high) {  // binary search
    mid = low + ((high - low) >> 1);
    if (comparator(array[mid].first, key) == -1) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return low;  // low = high
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) { return array[index]; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int size = GetSize();
  assert(size <= GetMaxSize());
  if (size == 0) {  // empty
    array[size] = {key, value};
  } else if (comparator(key, array[size - 1].first) == 1) {  // key > the last key in array
    array[size] = {key, value};
  } else if (comparator(key, array[0].first) < 0) {  // key < the first key
    memmove(array + 1, array, static_cast<size_t>(sizeof(MappingType) * size));
    array[0] = {key, value};
  } else {  // in the middle
    int where = KeyIndex(key, comparator);
    memmove(array + where + 1, array + where, static_cast<size_t>((size - where) * sizeof(MappingType)));
    array[where] = {key, value};
  }
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half = (GetSize() + 1) / 2;
  MappingType *src = array + GetSize() - half;
  recipient->CopyNFrom(src, half);
  IncreaseSize(-1 * half);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  assert(GetSize() == 0);
  memcpy(array, items, static_cast<size_t>(sizeof(MappingType) * size));
  IncreaseSize(size);
}



/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int size = GetSize();
  if (size == 0 || comparator(key, array[0].first) < 0 || comparator(key, array[size - 1].first) > 0) {
    return false;
  }
  int where = KeyIndex(key, comparator);
  if (comparator(key, array[where].first) == 0) {
    *value = array[where].second;
    return true;
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) 
{ 
    int size = GetSize();
    if(size == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(size - 1)) > 0){
        return size;
    }
    int index = KeyIndex(key, comparator);
    if(comparator(key, KeyAt(index)) == 0){
        memmove(array + index, array + index + 1, static_cast<size_t>((size - index - 1) * sizeof(MappingType)));
        size--;
        IncreaseSize(-1);
    }
    return size; 
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, int index_in_parent, BufferPoolManager*) 
{
    recipient->CopyAllFrom(array, GetSize());
    // if(index_in_parent == 0){ //reipient is the right sibling of this node
    //     //没办法获取到前一个的节点，无法更新
    // }
    // else{   //reipient is the left sibling of this node
    //     recipient->SetNextPageId(GetNextPageId());
    // }
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) 
{
    int current_size = GetSize();
    assert(size + current_size <= GetMaxSize());
    memcpy(array + current_size, items, static_cast<size_t>(size * sizeof(MappingType)));
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, BufferPoolManager* buffer_pool_manager) 
{
    recipient->CopyLastFrom(array[0]);
    memmove(array, array + 1, static_cast<size_t>((GetSize() - 1) * sizeof(MappingType)));
    IncreaseSize(-1);
    auto page = buffer_pool_manager->FetchPage(GetParentPageId());
    assert(page);
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), array[0].first);
    buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) 
{
    int size = GetSize();
    assert(size < GetMaxSize());
    array[size] = item;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex, BufferPoolManager* buffer_pool_manager)
{
    int size = GetSize();
    assert(size > GetMinSize());
    recipient->CopyFirstFrom(array[size - 1], parentIndex, buffer_pool_manager);
    IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item, int parentIndex, BufferPoolManager* buffer_pool_manager) 
{
    int size = GetSize();
    assert(size < GetMaxSize());
    memmove(array + 1, array, static_cast<size_t>(size * sizeof(MappingType)));
    array[0] = item;
    IncreaseSize(1);

    auto page = buffer_pool_manager->FetchPage(GetParentPageId());
    assert(page);
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    parent_node->SetKeyAt(parentIndex, array[0].first);
    buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
