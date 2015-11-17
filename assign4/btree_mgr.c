//
//  btree_mgr.c
//  assign4
//
//  Created by Dany on 11/9/15.
//  Copyright © 2015 Dany. All rights reserved.
//

#include <string.h>
#include <stdlib.h>
#include "btree_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
//#include "test_helper.h"

//int PAGE_SIZE = 4096; //debug

typedef struct RM_BtreeNode {
    void **ptrs;
    Value *keys;
    struct RM_BtreeNode *paptr;
    int KeyCounts;//#keys
    int pos; //for tree walk
    bool isLeaf;
} RM_BtreeNode;

typedef struct RM_bTree_mgmtData{
    int maxKeyNum;
    int numEntries;
    BM_BufferPool *bp;
}RM_bTree_mgmtData;

typedef struct RM_BScan_mgmt{
    int totalScan;
    int index;
    RM_BtreeNode *cur;
}RM_BScan_mgmt;

RM_BtreeNode *root = NULL;
int numNodValue = 0;
int sizeofNod = 0;
int globalPos = 0;
char *sv = NULL;
char *sv2 = NULL;
Value empty;

//------------Customized

bool strNoLarger(char *c1, char *c2){
    if (strlen(c1)<strlen(c2))
        return true;
    if (strlen(c1)>strlen(c2))
        return false;
    return (strcmp(c1, c2)<=0);
}

RM_BtreeNode *createNewNod(RM_BtreeNode *thisNode)
{
    if (thisNode == NULL)
        globalPos = -1;
    
    RM_BtreeNode *bTreeNode;
    globalPos = 0;
    
    bTreeNode = (RM_BtreeNode *)malloc(sizeof(RM_BtreeNode));
    bTreeNode->ptrs = malloc(sizeofNod * sizeof(void *));
    bTreeNode->keys = malloc((sizeofNod - 1) * sizeof(Value));
    
    thisNode = NULL;
    
    bTreeNode->paptr = NULL;
    bTreeNode->KeyCounts = 0;
    bTreeNode->isLeaf = FALSE;
    
    numNodValue += 1;
    
    
    
    return bTreeNode;
}

RC insertParent(RM_BtreeNode *left, RM_BtreeNode *right, Value key)
{
    RM_BtreeNode *paptr = left->paptr;
    int index = 0;
    int i = 0;
    
    if (paptr == NULL){
        //create parent
        RM_BtreeNode *NewRoot;
        
        NewRoot = createNewNod(NewRoot);
        NewRoot->keys[0] = key;
        NewRoot->KeyCounts = 1;
        NewRoot->ptrs[0] = left;
        NewRoot->ptrs[1] = right;
        left->paptr = NewRoot;
        right->paptr = NewRoot;
        
        root = NewRoot;
        return RC_OK;
    }
    else{
        //then it has parent
        while ( index < paptr->KeyCounts && paptr->ptrs[index] != left) index++;
        globalPos = paptr->pos;
        if (paptr->KeyCounts < sizeofNod - 1){
            //have empty slot
            for (int i = paptr->KeyCounts;i > index;i--){
                paptr->keys[i] = paptr->keys[i-1];
                globalPos = paptr->pos;
                paptr->ptrs[i+1] = paptr->ptrs[i];
            }
            paptr->keys[index] = key;
            paptr->ptrs[index+1] = right;
            paptr->KeyCounts += 1;
            
            return RC_OK;
        }
    }
    //then no empty space, split the node
    i = 0;
    int middleLoc;
    RM_BtreeNode **tempNode, *newNode;
    Value *tempKeys;
    
    tempNode = malloc((sizeofNod + 1) * sizeof(RM_BtreeNode *));
    tempKeys = malloc(sizeofNod * sizeof(Value));
    
    for (i=0; i < sizeofNod + 1;i++){
        if (i < index + 1)
            tempNode[i] = paptr->ptrs[i];
        else if (i == index + 1)
            tempNode[i] = right;
        else{
            globalPos = paptr->pos;
            tempNode[i] = paptr->ptrs[i-1];
        }
    }
    for (i = 0;i < sizeofNod;i++){
        if (i < index)
            tempKeys[i] = paptr->keys[i];
        else if (i == index)
            tempKeys[i] = key;
        else{
            globalPos = tempKeys[i].v.intV;
            tempKeys[i] = paptr->keys[i-1];
        }
    }
    
    if (sizeofNod % 2 == 0)
        middleLoc = sizeofNod / 2;
    else
        middleLoc = sizeofNod / 2 + 1;
    paptr->KeyCounts = middleLoc - 1;
    for (i = 0;i < middleLoc - 1;i++){
        paptr->ptrs[i] = tempNode[i];
        paptr->keys[i] = tempKeys[i];
    }
    paptr->ptrs[i] = tempNode[i];
    newNode = createNewNod(newNode);
    newNode->KeyCounts = sizeofNod - middleLoc;
    for (i=middleLoc;i <= sizeofNod;i++){
        newNode->ptrs[i - middleLoc] = tempNode[i];
        globalPos = newNode->pos;
        newNode->keys[i - middleLoc] = tempKeys[i];
    }
    newNode->paptr = paptr->paptr;
    
    Value t;
    t = tempKeys[middleLoc - 1];
    free(tempNode);
    tempNode = NULL;
    free(tempKeys);
    tempKeys = NULL;
    
    return insertParent(paptr, newNode, t);
}

RC deleteNode(RM_BtreeNode *bTreeNode, int index)
{
    int position, i, j;
    RM_BtreeNode *brother;
    //reduce the number of key value
    bTreeNode->KeyCounts--;
    int NumKeys = bTreeNode->KeyCounts;
    if (bTreeNode->isLeaf){
        //remove
        free(bTreeNode->ptrs[index]);
        bTreeNode->ptrs[index] = NULL;
        //re-order
        for (i = index; i < NumKeys;i++){
            bTreeNode->keys[i] = bTreeNode->keys[i + 1];
            globalPos = bTreeNode->pos;
            bTreeNode->ptrs[i] = bTreeNode->ptrs[i + 1];
        }
        bTreeNode->keys[i] = empty;
        bTreeNode->ptrs[i] = NULL;
    }
    else{
        //not leaf
        for (i = index - 1;i < NumKeys;i++){
            bTreeNode->keys[i] = bTreeNode->keys[i + 1];
            globalPos = bTreeNode->pos;
            bTreeNode->ptrs[i + 1] = bTreeNode->ptrs[i + 2];
        }
        bTreeNode->keys[i] = empty;
        bTreeNode->ptrs[i + 1] = NULL;
    }
    //finished rm&re-order
    if (bTreeNode->isLeaf)
        i = sizeofNod / 2;
    else
        i = (sizeofNod - 1) / 2;
    if (NumKeys >= i)
        return RC_OK;
    //then deal with underflow
    if (bTreeNode == root){
        if (root->KeyCounts > 0)
            return RC_OK;
        //root has no key left
        RM_BtreeNode *newRoot = NULL;
        if (!root->isLeaf){
            //only one child left
            newRoot = root->ptrs[0];
            newRoot->paptr = NULL;
        }
        
        free(root->keys);
        root->keys = NULL;
        free(root->ptrs);
        root->ptrs = NULL;
        free(root);
        root = NULL;
        numNodValue -= 1;
        root = newRoot;
        
        return RC_OK;
    }
    //then it's not root
    RM_BtreeNode *parentNode = bTreeNode->paptr;
    position = 0;
    
    while(position < parentNode->KeyCounts && parentNode->ptrs[position] != bTreeNode) position++;
    
    if (position == 0)//leftmost
        brother = parentNode->ptrs[1];
    else//regular
        brother = parentNode->ptrs[position - 1];
    if (bTreeNode->isLeaf)
        i = sizeofNod - 1;
    else
        i = sizeofNod - 2;
    //if can merge two nodes
    if (brother->KeyCounts + NumKeys <= i){
        //merging
        if (position == 0) {
            RM_BtreeNode *temp;
            temp = bTreeNode;
            bTreeNode = brother;
            brother = temp;
            position = 1;
            NumKeys = bTreeNode->KeyCounts;
        }
        i = brother->KeyCounts;
        if (!bTreeNode->isLeaf){
            brother->keys[i] = parentNode->keys[position - 1];
            i += 1;
            NumKeys += 1;
        }
        for (j=0;j<NumKeys;j++){
            brother->keys[i] = bTreeNode->keys[j];
            globalPos = brother->pos;
            brother->ptrs[i] = bTreeNode->ptrs[j];
            bTreeNode->keys[j] = empty;
            bTreeNode->ptrs[j] = NULL;
            i++;
        }
    
        brother->KeyCounts += NumKeys;
        brother->ptrs[sizeofNod - 1] = bTreeNode->ptrs[sizeofNod - 1];
        
        numNodValue -= 1;
        free(bTreeNode->keys);
        bTreeNode->keys = NULL;
        free(bTreeNode->ptrs);
        bTreeNode->ptrs = NULL;
        free(bTreeNode);
        bTreeNode = NULL;
        
        return deleteNode(parentNode, position);
    }
    //get one from brother
    int brotherNumKeys;
    if (position != 0) {
        //get one from left
        if (!bTreeNode->isLeaf)
            bTreeNode->ptrs[NumKeys + 1] = bTreeNode->ptrs[NumKeys];
        //shift to right by 1
        for (i = NumKeys; i > 0; i--){
            bTreeNode->keys[i] = bTreeNode->keys[i - 1];
            globalPos = bTreeNode->pos;
            bTreeNode->ptrs[i] = bTreeNode->ptrs[i - 1];
        }
        //i=0
        if (bTreeNode->isLeaf){
            brotherNumKeys = brother->KeyCounts - 1;
            bTreeNode->keys[0] = brother->keys[brotherNumKeys];
            parentNode->keys[position - 1] = bTreeNode->keys[0];
        }
        else{
            brotherNumKeys = brother->KeyCounts;
            bTreeNode->keys[0] = parentNode->keys[position - 1];
            parentNode->keys[position - 1] = brother->keys[brotherNumKeys - 1];
        }
        bTreeNode->ptrs[0] = brother->ptrs[brotherNumKeys];
        brother->keys[brotherNumKeys] = empty;
        brother->ptrs[brotherNumKeys] = NULL;
    }
    else {
        int temp = brother->KeyCounts;
        //get one from right
        if (bTreeNode->isLeaf){
            bTreeNode->keys[NumKeys] = brother->keys[0];
            bTreeNode->ptrs[NumKeys] = brother->ptrs[0];
            parentNode->keys[0] = brother->keys[1];
        }
        else {
            bTreeNode->keys[NumKeys] = parentNode->keys[0];
            bTreeNode->ptrs[NumKeys + 1] = brother->ptrs[0];
            parentNode->keys[0] = brother->keys[0];
        }
        //shift to left by 1
        for (i = 0; i < temp; i++){
            brother->keys[i] = brother->keys[i + 1];
            globalPos = brother->KeyCounts;
            brother->ptrs[i] = brother->ptrs[i + 1];
        }
        brother->ptrs[brother->KeyCounts] = NULL;
        brother->keys[brother->KeyCounts] = empty;
    }
    
    bTreeNode->KeyCounts++;
    brother->KeyCounts--;
    return RC_OK;
}

// init and shutdown index manager
RC initIndexManager (void *mgmtData)
{
    root = NULL;
    numNodValue = 0;
    sizeofNod = 0;
    empty.dt = DT_INT;
    empty.v.intV = 0;
    return RC_OK;
}
RC shutdownIndexManager ()
{
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n)
{
    if(idxId == NULL) return RC_IM_KEY_NOT_FOUND;
    RC rc;
    
    rc = createPageFile(idxId);
    if (rc != RC_OK)
        return rc;
    SM_FileHandle fhandle;
    rc = openPageFile(idxId, &fhandle);
    if (rc != RC_OK)
        return rc;
    
    SM_PageHandle pageData = (SM_PageHandle)malloc(sizeof(PAGE_SIZE));
    memcpy(pageData, &keyType, sizeof(int));
    pageData += sizeof(int);
    memcpy(pageData, &n, sizeof(int));
    pageData -= sizeof(int);
    
    rc = writeCurrentBlock(&fhandle, pageData);
    if (rc != RC_OK)
        return rc;
    rc = closePageFile(&fhandle);
    if (rc != RC_OK)
        return rc;
    
    free(pageData);
    pageData = NULL;
    
    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
    if(idxId == NULL) return RC_IM_KEY_NOT_FOUND;
    RC rc;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    *tree = (BTreeHandle *) malloc (sizeof(BTreeHandle));
    
    rc = initBufferPool(bm, idxId, 10, RS_CLOCK, NULL);
    if (rc != RC_OK)
        return rc;
    rc = pinPage(bm, page, 0);
    if (rc != RC_OK)
        return rc;
    
    int type;
    memcpy(&type, page->data, sizeof(int));
    (*tree)->keyType = (DataType)type;
    page->data += sizeof(int);
    int n;
    memcpy(&n, page->data, sizeof(int));
    page->data -= sizeof(int);
    
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *) malloc (sizeof(RM_bTree_mgmtData));
    bTreeMgmt->numEntries = 0;
    bTreeMgmt->maxKeyNum = n;
    bTreeMgmt->bp = bm;
    (*tree)->mgmtData = bTreeMgmt;
    
    free(page);
    page = NULL;
    
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree)
{
    if(tree == NULL) return RC_IM_KEY_NOT_FOUND;
    RC rc;
    
    tree->idxId = NULL;
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
    
    rc = shutdownBufferPool(bTreeMgmt->bp);
    if (rc != RC_OK)
        return rc;
    
    free(bTreeMgmt);
    bTreeMgmt = NULL;
    free(tree);
    tree = NULL;
    free(root);
    root = NULL;
    
    return RC_OK;
}

RC deleteBtree (char *idxId)
{
    if(idxId == NULL) return RC_IM_KEY_NOT_FOUND;
    
    RC rc = destroyPageFile(idxId);
    if (rc != RC_OK)
        return rc;
    
    return RC_OK;
}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    if(tree == NULL) return RC_IM_KEY_NOT_FOUND;
    
    (*result) = numNodValue;
    
    return RC_OK;
}
RC getNumEntries (BTreeHandle *tree, int *result)
{
    if(tree == NULL) return RC_IM_KEY_NOT_FOUND;
    
    (*result) = ((RM_bTree_mgmtData *)tree->mgmtData)->numEntries;
    
    return RC_OK;
}
RC getKeyType (BTreeHandle *tree, DataType *result)
{
    if(tree == NULL) return RC_IM_KEY_NOT_FOUND;
    
    (*result) = tree->keyType;
    
    return RC_OK;
}

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    
    if ((tree == NULL)||(key == NULL)||(root == NULL)) return RC_IM_KEY_NOT_FOUND;
    
    RM_BtreeNode *leaf;
    int i = 0;
    
    //findleaf
    leaf = root;
    while (!leaf->isLeaf){
        //--
        sv = serializeValue(&leaf->keys[i]);
        sv2 = serializeValue(key);
        while ((i < leaf->KeyCounts) && strNoLarger(sv, sv2)){
            free(sv);
            sv = NULL;
            i++;
            if (i < leaf->KeyCounts) sv = serializeValue(&leaf->keys[i]);
        }
        free(sv);
        sv = NULL;
        free(sv2);
        sv2 = NULL;
        //--
        leaf = (RM_BtreeNode *)leaf->ptrs[i];
        i = 0;
    }
    //--
    sv = serializeValue(&leaf->keys[i]);
    sv2 = serializeValue(key);
    while ((i < leaf->KeyCounts) && (strcmp(sv, sv2)!=0)){
        free(sv);
        sv = NULL;
        i++;
        if (i < leaf->KeyCounts) sv = serializeValue(&leaf->keys[i]);
    }
    free(sv);
    sv = NULL;
    free(sv2);
    sv2 = NULL;
    //--
    
    if (i >= leaf->KeyCounts)
        return RC_IM_KEY_NOT_FOUND;
    else{
        result->page = ((RID *)leaf->ptrs[i])->page;
        result->slot = ((RID *)leaf->ptrs[i])->slot;
        return RC_OK;
    }
}
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    if ((tree == NULL)||(key == NULL)) return RC_IM_KEY_NOT_FOUND;
    
    RM_BtreeNode *leaf;
    int i = 0;
    
    //findleaf
    leaf = root;
    if (leaf != NULL)
    {
        while (!leaf->isLeaf)
        {
            //--
            sv = serializeValue(&leaf->keys[i]);
            sv2 = serializeValue(key);
            while ((i < leaf->KeyCounts) && strNoLarger(sv, sv2)){
                free(sv);
                sv = NULL;
                i++;
                if (i < leaf->KeyCounts) sv = serializeValue(&leaf->keys[i]);
            }
            free(sv);
            sv = NULL;
            free(sv2);
            sv2 = NULL;
            //--
            leaf = (RM_BtreeNode *)leaf->ptrs[i];
            i = 0;
        }
    }
    
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
    bTreeMgmt->numEntries += 1;
    
    if (!leaf)
    {
        sizeofNod = bTreeMgmt->maxKeyNum + 1;
        
        root = createNewNod(root);
        RID *rec = (RID *) malloc (sizeof(RID));
        rec->page = rid.page;
        rec->slot = rid.slot;
        root->ptrs[0] = rec;
        root->keys[0] = *key;
        root->ptrs[sizeofNod - 1] = NULL;
        root->isLeaf = true;
        root->KeyCounts++;
    }
    else
    {
        int index = 0;
        //--
        sv = serializeValue(&leaf->keys[index]);
        sv2 = serializeValue(key);
        while ((index < leaf->KeyCounts) && strNoLarger(sv, sv2)){
            free(sv);
            sv = NULL;
            index++;
            if (index < leaf->KeyCounts) sv = serializeValue(&leaf->keys[index]);
        }
        free(sv);
        sv = NULL;
        free(sv2);
        sv2 = NULL;
        //--
        
        if (leaf->KeyCounts < sizeofNod - 1)
        {
            //empty slot
            for (int i = leaf->KeyCounts;i > index;i--){
                leaf->keys[i] = leaf->keys[i-1];
                globalPos = leaf->pos;
                leaf->ptrs[i] = leaf->ptrs[i-1];
            }
            RID *rec = (RID *) malloc (sizeof(RID));
            rec->page = rid.page;
            rec->slot = rid.slot;
            leaf->keys[index] = *key;
            leaf->ptrs[index] = rec;
            leaf->KeyCounts += 1;
        }
        else
        {
            RM_BtreeNode *newLeafNod;
            RID **NodeRID;
            Value *NodeKeys;
            int middleLoc = 0;
            NodeRID = malloc(sizeofNod * sizeof(RID *));
            NodeKeys = malloc(sizeofNod * sizeof(Value));
            //full node
            for (i=0;i < sizeofNod;i++)
            {
                if (i == index){
                    RID *newValue = (RID *) malloc (sizeof(RID));
                    newValue->page = rid.page;
                    newValue->slot = rid.slot;
                    NodeRID[i] = newValue;
                    NodeKeys[i] = *key;
                }
                else if (i < index){
                    NodeRID[i] = leaf->ptrs[i];
                    globalPos = NodeRID[i]->page;
                    NodeKeys[i] = leaf->keys[i];
                    middleLoc = sizeofNod % 2 == 0;
                }
                else{//i > index
                    middleLoc = globalPos;
                    NodeRID[i] = leaf->ptrs[i-1];
                    globalPos = NodeRID[i]->page;
                    NodeKeys[i] = leaf->keys[i-1];
                }
            }
            
            middleLoc = sizeofNod / 2 + 1;
            //old leaf
            for (i=0;i < middleLoc;i++)
            {
                leaf->ptrs[i] = NodeRID[i];
                leaf->keys[i] = NodeKeys[i];
            }
            //new leaf
            newLeafNod = createNewNod(newLeafNod);
            newLeafNod->isLeaf = true;
            newLeafNod->paptr = leaf->paptr;
            newLeafNod->KeyCounts = sizeofNod - middleLoc;
            for (i=middleLoc;i < sizeofNod;i++){
                newLeafNod->ptrs[i - middleLoc] = NodeRID[i];
                newLeafNod->keys[i - middleLoc] = NodeKeys[i];
            }
            //add to link list
            newLeafNod->ptrs[sizeofNod - 1] = leaf->ptrs[sizeofNod - 1];
            leaf->KeyCounts = middleLoc;
            leaf->ptrs[sizeofNod - 1] = newLeafNod;
            
            free(NodeRID);
            NodeRID = NULL;
            free(NodeKeys);
            NodeKeys = NULL;
            
            RC rc = insertParent(leaf, newLeafNod, newLeafNod->keys[0]);
            if (rc != RC_OK)
                return rc;
        }
    }
    
    tree->mgmtData = bTreeMgmt;
    return RC_OK;
}
RC deleteKey (BTreeHandle *tree, Value *key)
{
    if ((tree == NULL)||(key == NULL)) return RC_IM_KEY_NOT_FOUND;
    RC rc;
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
    bTreeMgmt->numEntries -= 1;
    RM_BtreeNode *leaf;
    int i = 0;
    //findleaf then delete
    leaf = root;
    if (leaf != NULL)
    {
        while (!leaf->isLeaf)
        {
            //--
            sv = serializeValue(&leaf->keys[i]);
            sv2 = serializeValue(key);
            while ((i < leaf->KeyCounts) && strNoLarger(sv, sv2)){
                free(sv);
                sv = NULL;
                i++;
                if (i < leaf->KeyCounts) sv = serializeValue(&leaf->keys[i]);
            }
            free(sv);
            sv = NULL;
            free(sv2);
            sv2 = NULL;
            //--
            leaf = (RM_BtreeNode *)leaf->ptrs[i];
            i = 0;
        }
        //--
        sv = serializeValue(&leaf->keys[i]);
        sv2 = serializeValue(key);
        while ((i < leaf->KeyCounts) && (strcmp(sv, sv2)!=0)){
            free(sv);
            sv = NULL;
            i++;
            if (i < leaf->KeyCounts) sv = serializeValue(&leaf->keys[i]);
        }
        free(sv);
        sv = NULL;
        free(sv2);
        sv2 = NULL;
        //--
        if (i < leaf->KeyCounts)
        {
            rc = deleteNode(leaf, i);
            if (rc != RC_OK)
                return rc;
        }
    }
    
    tree->mgmtData = bTreeMgmt;
    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    if(tree == NULL) return RC_IM_KEY_NOT_FOUND;
    
    *handle = (BT_ScanHandle *) malloc (sizeof(BT_ScanHandle));
    (*handle)->tree = tree;
    RM_BScan_mgmt *scanMgmt = (RM_BScan_mgmt *) malloc (sizeof(RM_BScan_mgmt));
    scanMgmt->cur = NULL;
    scanMgmt->index = 0;
    scanMgmt->totalScan = 0;
    (*handle)->mgmtData = scanMgmt;
    
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if(handle == NULL) return RC_IM_KEY_NOT_FOUND;
    RC rc;
    RM_BScan_mgmt *scanMgmt = (RM_BScan_mgmt *) handle->mgmtData;
    int totalRes = 0;
    
    rc = getNumEntries(handle->tree, &totalRes);
    if (rc != RC_OK)
        return rc;
    if(scanMgmt->totalScan >= totalRes){
        return RC_IM_NO_MORE_ENTRIES;
    }
    
    RM_BtreeNode *leaf = root;
    if(scanMgmt->totalScan == 0){
        while (!leaf->isLeaf)
            leaf = leaf->ptrs[0];
        scanMgmt->cur = leaf;
    }
    
    if(scanMgmt->index == scanMgmt->cur->KeyCounts){
        scanMgmt->cur = (RM_BtreeNode *)scanMgmt->cur->ptrs[((RM_bTree_mgmtData *)handle->tree->mgmtData)->maxKeyNum];
        scanMgmt->index = 0;
    }
    
    RID *ridRes = (RID *) malloc (sizeof(RID));
    ridRes = (RID *)scanMgmt->cur->ptrs[scanMgmt->index];
    scanMgmt->index++;
    scanMgmt->totalScan++;
    handle->mgmtData = scanMgmt;
    
    result->page = ridRes->page;
    result->slot = ridRes->slot;
    
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle)
{
    RM_BScan_mgmt *mgmt = handle->mgmtData;
    free(mgmt);
    mgmt = NULL;
    free(handle);
    handle = NULL;
    
    return RC_OK;
}

// debug and test functions

int DFS(RM_BtreeNode *bTreeNode)
{
    bTreeNode->pos = globalPos++;
    if (bTreeNode->isLeaf)
        return 0;
    for (int i=0;i<=bTreeNode->KeyCounts;i++)
    {
        DFS(bTreeNode->ptrs[i]);
    }
    return 0;
}

int walk(RM_BtreeNode *bTreeNode, char *result)
{
    char *line = (char *)malloc(100);
    //int len = 100;
    char *t = (char *) malloc(10);
    int i = 0;
    strcpy(line, "(");
    sprintf(t, "%d", bTreeNode->pos);
    strcat(line, t);
    strcat(line, ")[");
    if (bTreeNode->isLeaf){
        //RID,Key,RID,...
        i = 0;
        for (i=0;i<bTreeNode->KeyCounts;i++){
            //RID
            sprintf(t, "%d.%d,", ((RID *)bTreeNode->ptrs[i])->page, ((RID *)bTreeNode->ptrs[i])->slot);
            strcat(line, t);
            //Key
            //sprintf(t, "%d,", bTreeNode->keys[i]);
            //strcat(line, t);
            sv = serializeValue(&bTreeNode->keys[i]);
            strcat(line, sv);
            free(sv);
            sv = NULL;
            strcat(line, ",");
        }
        if (bTreeNode->ptrs[sizeofNod - 1] != NULL){
            //Pos
            i = ((RM_BtreeNode *)bTreeNode->ptrs[sizeofNod - 1])->pos;
            sprintf(t, "%d", i);
            strcat(line, t);
        }
        else{
            line[strlen(line)-1] = 0;//delete ","
        }
        strcat(line, "]\n");
    }
    else{
        //not leaf
        i = 0;
        for (i=0;i<bTreeNode->KeyCounts;i++){
            sprintf(t, "%d,", ((RM_BtreeNode *)bTreeNode->ptrs[i])->pos);
            strcat(line, t);
            
            sv = serializeValue(&bTreeNode->keys[i]);
            strcat(line, sv);
            free(sv);
            sv = NULL;
            strcat(line, ",");
        }
        if (((RM_BtreeNode *) bTreeNode->ptrs[i]) != NULL){
            sprintf(t, "%d", ((RM_BtreeNode *)bTreeNode->ptrs[i])->pos);
            strcat(line, t);
        }
        else{
            line[strlen(line)-1] = 0;//delete ","
        }
        strcat(line, "]\n");
    }
    strcat(result,line);
    free(line);
    line = NULL;
    free(t);
    t = NULL;
    if (!bTreeNode->isLeaf){
        for (int i=0;i<=bTreeNode->KeyCounts;i++)
        {
            walk(bTreeNode->ptrs[i],result);
        }
    }
    return 0;
}

char *printTree (BTreeHandle *tree)
{
    if (root == NULL)
        return NULL;
    globalPos = 0;
    int lenth = DFS(root);
    lenth = 1000;
    char *result = malloc(lenth*sizeof(char));
    walk(root, result);
    return result;
}