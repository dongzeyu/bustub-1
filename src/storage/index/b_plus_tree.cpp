//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) 
{
    first_call = true;
    height_ = 0;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }
  auto leaf_page = FindLeafPage(key, false, transaction, Operation::SEARCH);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  ValueType tmp;
  if (leaf->Lookup(key, &tmp, comparator_)) {
    result->push_back(tmp);
    UnlockPage(leaf_page, transaction, Operation::SEARCH);
    return true;
  } else {
    UnlockPage(leaf_page, transaction, Operation::SEARCH);
    return false;
  }
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction)
{
  /*
  //Tree is empty, start a new leaf(also root)
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  //else find key in leaf
  auto leaf_page = FindLeafPage(key, false);   //Page* --> BPlusTreeLeafPage*
  auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(leaf_page);
  LOG_DEBUG("find leaf page: %d", leaf->GetPageId());
  ValueType ret_val;
  // key already exist
  if(leaf->Lookup(key, &ret_val, comparator_)) {   //FIXME!!!
    //neeed to unpin
    //buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }

  //case 1: no need to split
  if(leaf->GetSize() < leaf->GetMaxSize()){
    leaf->Insert(key, value, comparator_);
  }
  //case 2: need to split
  else{
    leaf->Insert(key, value, comparator_);
    //auto new_leaf = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);
    auto new_leaf = Split(leaf);
    //new_leaf->SetPageType(IndexPageType::LEAF_PAGE);
    leaf->SetNextPageId(new_leaf->GetPageId());
    //insert the middel key to parent

    //the middle key is the first key of new_leaf
    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
  }
  return true;
  */
  std::call_once(flag_, &BPLUSTREE_TYPE::StartNewTree, this, key, value, transaction);

  // 这里维护一个最大的key，求end的时候用到
  if (first_call) {
    max_key = key;
    first_call = false;
  } else {
    if (comparator_(key, max_key) > 0) {
      max_key = key;
    }
  }

  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value, Transaction *txn) {
    assert(IsEmpty());

    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (!page) {
        // throw "out of memory in StartNewTree";
        throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
    }
    LockPage(page, txn, Operation::INSERT);
    auto root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    UpdateRootPageId(true);
    root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    root->SetPageType(IndexPageType::LEAF_PAGE);
    assert(!IsEmpty());
    root->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    txn->GetPageSet()->pop_front();
    height_ = 1;
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto leaf_page = FindLeafPage(key, false, transaction, Operation::INSERT);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  ValueType tmp;

  if (leaf->Lookup(key, &tmp, comparator_)) {  // already in tree
    UnlockParentPage(leaf_page, transaction, Operation::INSERT);
    UnlockPage(leaf_page, transaction, Operation::INSERT);
    return false;
  } else {
    if (leaf->GetSize() < leaf->GetMaxSize()) {  // leaf is not full
      leaf->Insert(key, value, comparator_);
      UnlockPage(leaf_page, transaction, Operation::INSERT);
      if (transaction) {
        assert(transaction->GetPageSet()->empty());
      }
    } else {  // leaf is full
      auto new_leaf_node = Split(leaf);
      assert(new_leaf_node->IsLeafPage());
      // update linked list
      new_leaf_node->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_leaf_node->GetPageId());

      new_leaf_node->SetParentPageId(leaf->GetParentPageId());
      KeyType middle_one = new_leaf_node->KeyAt(0);  // the first key of the new node

      InsertIntoParent(leaf, middle_one, new_leaf_node, transaction);

      if (comparator_(key, middle_one) < 0) {
        leaf->Insert(key, value, comparator_);
      } else {
        new_leaf_node->Insert(key, value, comparator_);
      }

      UnlockParentPage(leaf_page, transaction, Operation::INSERT);
      UnlockPage(leaf_page, transaction, Operation::INSERT);
      if (transaction) {
        assert(transaction->GetPageSet()->empty());
      }
    }
    return true;
  }  // else
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // ask a new page
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (!new_page) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
  }

  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->Init(new_page_id, INVALID_PAGE_ID, node->GetMaxSize());

  // LOG_DEBUG("new_node max size: %d", new_node->GetMaxSize());

  node->MoveHalfTo(new_node, buffer_pool_manager_);
  split_count_++;
  assert(new_page->GetPinCount() == 1 && new_page->GetPageId() == new_node->GetPageId());

  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // current node is root, create a new root
  if (old_node->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);  // Page*
    if (!page) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
    }
    page->WLatch();
    transaction->GetPageSet()->push_front(page);

    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    // new_root->SetPageType(IndexPageType::INTERNAL_PAGE);
    LOG_INFO("new_root's size before: %d", new_root->GetSize());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    assert(new_root->GetPageId() == root_page_id_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    LOG_INFO("new_root's size after: %d", new_root->GetSize());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(false);  // here is false, no need to insert pair into header_page
    split_count_++;
    LOG_INFO("root_id: %d", root_page_id_);
    
    height_++; //树高加一

    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
  } else {  // current node is not root
    auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (!parent_page) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
    }
    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
    // parent dont's need to split
    if (parent->GetSize() < parent->GetMaxSize()) {
      parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      // new_node->SetParentPageId(parent->GetPageId());
      // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    } else {  // split parent
      auto new_parent = Split(parent);
      new_parent->SetParentPageId(parent->GetParentPageId());
      KeyType middle_one = new_parent->KeyAt(0);

      //LOG_INFO("parent page id: %d", parent->GetPageId());
      //LOG_INFO("new_parent page id: %d", new_parent->GetPageId());

      //LOG_INFO("parent: %d", parent->GetPageId());
      for (int i = 0; i < parent->GetSize(); i++) {
        std::cout << parent->KeyAt(i) << ": " << parent->ValueAt(i) << std::endl;
      }

      //LOG_INFO("new_parent: %d", new_parent->GetPageId());
      for (int i = 0; i < new_parent->GetSize(); i++) {
        std::cout << new_parent->KeyAt(i) << ": " << new_parent->ValueAt(i) << std::endl;
      }

      if (comparator_(key, middle_one) < 0) {
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(parent->GetPageId());
      } else {
        new_parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(new_parent->GetPageId());
      }

      //LOG_INFO("parent: %d", parent->GetPageId());
      for (int i = 0; i < parent->GetSize(); i++) {
        std::cout << parent->KeyAt(i) << ": " << parent->ValueAt(i) << std::endl;
      }

      //LOG_INFO("new_parent: %d", new_parent->GetPageId());
      for (int i = 0; i < new_parent->GetSize(); i++) {
        std::cout << new_parent->KeyAt(i) << ": " << new_parent->ValueAt(i) << std::endl;
      }

      InsertIntoParent(parent, middle_one, new_parent, transaction);
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
 /*
 1. find the leaf page;
 2. delete key&value in leaf page;
 3. check the number of key after deletion, if less than the min size, need to Coalesce or Redistribute.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) 
{

    if(IsEmpty()) {
        throw Exception(ExceptionType::INVALID, "Tree is empty!");
        return ;
    }

    auto leaf_page = FindLeafPage(key, false, transaction, Operation::DELETE);

    assert(leaf_page != nullptr);
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page->GetData());

    int size_original = leaf->GetSize();

    int size_after_delete = leaf->RemoveAndDeleteRecord(key, comparator_);   // delete

    LOG_INFO("size after delete in page %d: %d", leaf->GetPageId(), size_after_delete);

    if(size_original == size_after_delete) {    // key not found
        UnlockParentPage(leaf_page, transaction, Operation::DELETE);
        UnlockPage(leaf_page, transaction, Operation::DELETE);
        return;
    } 
    //只有需要CoalesceOrRedistribute时，才会从父节点中删除相应的key,直接删除的话并不删除父节点中的key
    if(size_after_delete < leaf->GetMinSize()){  // need to coalesce
        bool res = CoalesceOrRedistribute(leaf, transaction);
        if(!res) {  // target leaf node is not deleted
            if(transaction){
                UnlockParentPage(leaf_page, transaction, Operation::DELETE);
            }
            UnlockPage(leaf_page, transaction, Operation::DELETE);
        } else{
            UnlockAllPage(transaction, Operation::DELETE);
        }
        if(transaction){
            assert(transaction->GetPageSet()->empty());
        }
    } else{   // no need to coalesce
        UnlockParentPage(leaf_page, transaction, Operation::DELETE);
        UnlockPage(leaf_page, transaction, Operation::DELETE);
    }

    for(auto it = transaction->GetDeletedPageSet()->begin(); it != transaction->GetDeletedPageSet()->end(); it++) {
        assert(buffer_pool_manager_->DeletePage(*it));
    }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
 /*
 1. if the node is root, adjust root;
 2. fetch sibling
 3. if the sum of number of keys in current node and sibling is no more than the max size, coalesce; otherwise, redistribute
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) 
{
    if (node->IsRootPage()){
        if(AdjustRoot(node)){
            transaction->AddIntoDeletedPageSet(node->GetPageId());
            height_--;
            return true;
        }
        else{
            return false;
        }
    }else {
        auto page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
        if(!page){
            throw Exception(ExceptionType::OUT_OF_MEMORY, "CoalesceOrRedistribute: out of memory");
        }
        auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
        /*
         Always get the left neighborhood, if current one is the leftmost one,  then get the right neighborhood.
        */
        //fetch sibling
        LOG_INFO("node->page_id: %d", node->GetPageId());
        int index_in_parent = parent->ValueIndex(node->GetPageId());
        LOG_INFO("index: %d", index_in_parent);
        if(index_in_parent == 0){   //current node is leftmost node, fetch right sibling
            auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index_in_parent + 1));
            assert(neighbor_page);
            N* neighbor_node = reinterpret_cast<N*>(neighbor_page->GetData());
            assert(node->IsLeafPage() == neighbor_node->IsLeafPage());
            // 可以直接合并
            if(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize()){
                //注意node与neighbor_node的顺序
                Coalesce(node, neighbor_node, parent, index_in_parent + 1, transaction);

                // if(neighbor_node->IsLeafPage()){
                //     LOG_INFO("leaf page!");
                //     //应该是这里死循环了
                //     Page* previous_page = FindPreviousPage(node->GetPageId());
                //     B_PLUS_TREE_LEAF_PAGE_TYPE* previous_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(previous_page->GetData());
                //     LOG_INFO("previous_node's page_id: %d", previous_node->GetPageId());
                //     previous_node->SetNextPageId(neighbor_node->GetPageId());
                // }
            }
            // 不能直接合并
            else{
                LOG_INFO("should Redistribute");
                Redistribute(neighbor_node, node, index_in_parent);
            }
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
            return false;
        }
        else{
            // always fetch left sibling
            auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index_in_parent - 1));
            assert(neighbor_page);
            N* neighbor_node = reinterpret_cast<N*>(neighbor_page->GetData());
            if(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize()){
                Coalesce(neighbor_node, node, parent, index_in_parent, transaction);
                buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
                buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
                return true;
            }
            else{
                LOG_INFO("should Redistribute");
                Redistribute(neighbor_node, node, index_in_parent);
                buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
                buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
                return false;
            }
        }
    }
}

/*找到page_id对应page的前一个page*/
INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::FindPreviousPage(page_id_t page_id)
{
    LOG_INFO("in FindPreviousPage");
    KeyType tmp;
    auto first_leaf_page = FindLeafPage(tmp, true, nullptr, Operation::SEARCH);
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(first_leaf_page->GetData());
    auto leaf_page = first_leaf_page;

    LOG_INFO("leaf_page: %d", leaf->GetPageId());
    while(leaf->GetNextPageId() != page_id){
        leaf_page = buffer_pool_manager_->FetchPage(leaf->GetNextPageId());
        leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
        LOG_INFO("leaf_page: %d", leaf->GetPageId());
    }
    LOG_INFO("find previous");
    return leaf_page;
}


/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
 /*
 1. move current node's  key&values to sibling;
 2. remove from parent;
 3. notify bpm to delete this page;
 4. check whether the parent after deletion need to coalesce or redistribution, if so, do it.
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *&neighbor_node, N *&node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent, int index,
                              Transaction *transaction)
{
    //LOG_INFO("node->page_id: %d", node->GetPageId());
    //LOG_INFO("index: %d", index);
    //assert(parent->ValueAt(index) == node->GetPageId());
    //int parent_index = index;
    //这个函数里要更新next_page_id
    int index_in_parent = index;
    node->MoveAllTo(neighbor_node, index_in_parent, buffer_pool_manager_); // append node's item to neighbor
    parent->Remove(index_in_parent);
    LOG_INFO("parent's size after remove key at %d: %d", index_in_parent, parent->GetSize());
    LOG_INFO("parent's child:");
    parent->show();

    transaction->AddIntoDeletedPageSet(node->GetPageId());

    //父节点这里加上等号
    if(parent->GetSize() < parent->GetMinSize()){
        return CoalesceOrRedistribute(parent, transaction);
    }

    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
 /*
 borrow one key&value from sibling; 
 index = 0 means neighbor is the right sibling, otherwise is the left sibling.
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index)
{
    if(index == 0){
        neighbor_node->MoveFirstToEndOf(node,  buffer_pool_manager_);
    }
    else{
        neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node)
{ 
    if(!old_root_node->IsLeafPage()){   //root is not leaf and only have one child
        if(old_root_node->GetSize() == 1){
            auto root_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(old_root_node);
            // let the only child be the new root
            root_page_id_ = root_node->ValueAt(0);
            UpdateRootPageId(false);
            auto page = buffer_pool_manager_->FetchPage(root_page_id_);
            auto new_root = reinterpret_cast<BPlusTreePage*>(page->GetData());
            new_root->SetParentPageId(INVALID_PAGE_ID);
            buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
            return true;
        }
        return false;
    } else {
        if(old_root_node->GetSize() == 0){  // the root is leaf node, which means the whole B plus tree is empty
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(false);
            return true;
        }
    }
    return false; 
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType tmp;
  auto leaf_page = FindLeafPage(tmp, true, nullptr, Operation::SEARCH);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  auto page_id = leaf->GetPageId();
  UnlockPage(leaf_page, nullptr, Operation::SEARCH);
  return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf_page = FindLeafPage(key, false, nullptr, Operation::SEARCH);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  auto page_id = leaf->GetPageId();
  int index = leaf->KeyIndex(key, comparator_);
  UnlockPage(leaf_page, nullptr, Operation::SEARCH);
  return INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node.
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
    auto iter = begin();
    while(!iter.isEnd()){
        iter.operator++();
    }
    return iter;
  // 找到最大的key对应的leaf
//   auto leaf_page = FindLeafPage(max_key, false, nullptr, Operation::SEARCH);
//   auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
//   // ensure this is the last key in last page
//   assert(leaf->GetNextPageId() == INVALID_PAGE_ID);
//   auto page_id = leaf->GetPageId();
//   int index = leaf->KeyIndex(max_key, comparator_);
//   assert(leaf->GetSize() == index + 1);
//   UnlockPage(leaf_page, nullptr, Operation::SEARCH);
//   return INDEXITERATOR_TYPE(page_id, index + 1, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Transaction *txn, Operation op) {
  if (IsEmpty()) {
    return nullptr;
  }

  if (op != Operation::SEARCH) {
    LockRoot();
  }

  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (!page) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
  }
  LockPage(page, txn, op);

  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    page_id_t parent_page_id = node->GetPageId(), child_page_id;

    if (leftMost) {
      //auto child = internal->array[0].second;
      child_page_id = internal->ValueAt(0);
    } else {
      child_page_id = internal->Lookup(key, comparator_);  // binary search key in this internal node
    }
    if (txn == nullptr) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    page = buffer_pool_manager_->FetchPage(child_page_id);
    if (!page) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
    }
    LockPage(page, txn, op);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    assert(node->GetParentPageId() == parent_page_id);

    if (txn != nullptr) {
      if ((op == Operation::SEARCH) || (op == Operation::INSERT && node->GetSize() < node->GetMaxSize()) ||
          (op == Operation::DELETE && node->GetSize() > node->GetMinSize())) {
        UnlockParentPage(page, txn, op);
      }
    }
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page *page, Transaction *txn, Operation op) {
  if (op == Operation::SEARCH) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  if (txn) {
    //txn->GetPageSet()->push_back(page);
    txn->AddIntoPageSet(page);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPage(Page *page, Transaction *txn, Operation op) {
  if (page->GetPageId() == root_page_id_) {
    UnLockRoot();
  }
  if (op == Operation::SEARCH) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  } else {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  if (txn) {
    txn->GetPageSet()->pop_front();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockAllPage(Transaction *txn, Operation op) {
  if (!txn) {
    return;
  }
  while (!txn->GetPageSet()->empty()) {
    auto front = txn->GetPageSet()->front();
    if (front->GetPageId() != INVALID_PAGE_ID) {
      if (op == Operation::SEARCH) {
        front->RUnlatch();
        buffer_pool_manager_->UnpinPage(front->GetPageId(), false);
      } else {
        if (front->GetPageId() == root_page_id_) {
          UnLockRoot();
        }
        front->WUnlatch();
        buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
      }
    }
    txn->GetPageSet()->pop_front();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockParentPage(Page *page, Transaction *txn, Operation op) {
  if (!txn) {
    return;
  }
  if (txn->GetPageSet()->empty()) {
    return;
  }
  if (page->GetPageId() == INVALID_PAGE_ID) {
    UnlockAllPage(txn, op);
  } else {
    while (!txn->GetPageSet()->empty() && txn->GetPageSet()->front()->GetPageId() != page->GetPageId()) {
      auto front = txn->GetPageSet()->front();
      if (front->GetPageId() != INVALID_PAGE_ID) {
        if (op == Operation::SEARCH) {
          front->RUnlatch();
          buffer_pool_manager_->UnpinPage(front->GetPageId(), false);
        } else {
          if (front->GetPageId() == root_page_id_) {
            UnLockRoot();
          }
          front->WUnlatch();
          buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
        }
      }
      txn->GetPageSet()->pop_front();
    }
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
