#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>
#include <stdlib.h>

typedef struct frame{
    int currpage; //the corresponding page in the file
    bool dirty;
    int fixCount;
    char data[PAGE_SIZE];
    struct frame *next;
    struct frame *prev;
}frame;

typedef struct priqueue{
    frame *pframe;
    struct priqueue *next;
}priqueue;

typedef struct buffer{ //use as a class
    int numFrames; // number of frames in the frame list
    int numRead; //for readIO
    int numWrite; //for writeIO
    void *stratData; //sizeof(void)=8 siszeof(int)=4; cheat here
    int pinnNum; //pinned frames
    priqueue *phead;
    frame *head;
    frame *pointer; //seems no use now
}buffer;

/* Custom Functions*/

bool alreadyPinned(BM_BufferPool *const bm, const PageNumber pageNum)
{
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    while (pt!=NULL)
    {
        if (pt->currpage==pageNum) return true;
        pt=pt->next;
    }
    return false;
}

/* Pinning Functions*/
RC pinFIFO (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    //load into memory using FIFO
    buffer *bf = bm->mgmtData;
    int count = 1;
    priqueue *ppt = bf->phead; //first one
    
    while (count<=bm->numPages)
    {
        if ((ppt->next==NULL)||(ppt->pframe->fixCount==0))
            break;
        ppt=ppt->next;
        count++;
    }
    if (count == bm->numPages+1)
        return RC_IM_NO_MORE_ENTRIES;
    
    SM_FileHandle *fHandle;
    RC rt_value = openPageFile(bm->pageFile, fHandle);
    if (rt_value!=RC_OK) return rt_value;
    
    if (ppt->pframe->dirty)
    {
        rt_value = writeBlock(ppt->pframe->currpage, fHandle, ppt->pframe->data);
        if (rt_value!=RC_OK) return rt_value;
        ppt->pframe->dirty = false;
        bf->numWrite++;
    }
    
    rt_value = readBlock(pageNum, fHandle, ppt->pframe->data);
    if (rt_value!=RC_OK) return rt_value;
    
    ppt->pframe->currpage = pageNum;
    ppt->pframe->fixCount++;
    page->pageNum = pageNum;
    page->data = ppt->pframe->data;
    
    closePageFile(fHandle);
    
    return RC_OK;
}

RC pinLRU (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum);
RC pinCLOCK (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum);
RC pinLFU (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum);
RC pinLRUK (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum);

/* Assignment Functions*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
    //error check
    if (numPages<=0) //input check
        return RC_WRITE_FAILED;
    //init bf:bookkeeping data
    buffer *bf = malloc(sizeof(buffer));
    priqueue *ppt;
    ppt->next=NULL;
    ppt->pframe=NULL;
    if (bf==NULL) return RC_WRITE_FAILED;
    bf->numFrames = numPages;
    bf->stratData = stratData;
    bf->pinnNum = 0;
    bf->numRead = 0;
    bf->numWrite = 0;
    bf->phead = ppt;
    //create list
    int i;
    frame *phead = malloc(sizeof(frame));
    if (phead==NULL) return RC_WRITE_FAILED;
    phead->currpage=0;
    phead->dirty=false;
    phead->fixCount=0;
    memset(phead->data,'\0',PAGE_SIZE);
    phead->prev=NULL;
    
    bf->head = phead;
    for (i=0; i<numPages; i++) {
        frame *pnew = malloc(sizeof(frame));
        if (pnew==NULL) return RC_WRITE_FAILED;
        pnew->currpage=0;
        pnew->dirty=false;
        pnew->fixCount=0;
        memset(pnew->data,'\0',PAGE_SIZE);
        pnew->next=NULL;
        bf->pointer->next=pnew;
        pnew->prev=bf->pointer;
        bf->pointer=pnew;
    }
    //circular list
    bf->pointer->next = bf->head;
    bf->head->prev = bf->pointer;
    
    //init bm
    bm->numPages = numPages;
    bm->pageFile = (char *)pageFileName;
    bm->strategy = strategy;
    bm->mgmtData = bf;
    
    bf=NULL;
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (bm->numPages<=0) return RC_IM_KEY_NOT_FOUND;
    //write dirty pages back to disk
    RC rt_value = forceFlushPool(bm);
    if (rt_value!=RC_OK) return rt_value;
    //free up resources
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    
    while (pt!=NULL)
    {
        pt = pt->next;
        free(bf->head);
        bf->head = pt;
    }
    free(bf);
    
    bm->numPages = 0;
    bm->pageFile = NULL;
    bm->mgmtData = NULL;
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    if (bm->numPages<=0) return RC_IM_KEY_NOT_FOUND;
    //write all dirty pages (fix count 0) to disk
    //pinned check
    buffer *bf = bm->mgmtData;
    if (bf->pinnNum>0) return RC_IM_NO_MORE_ENTRIES;
    
    SM_FileHandle *fHandle;
    RC rt_value = openPageFile(bm->pageFile, fHandle);
    if (rt_value!=RC_OK) return rt_value;

    frame *pt = bf->head;
    while (pt!=NULL)
    {
        if (pt->dirty)
        {
            rt_value = writeBlock(pt->currpage, fHandle, pt->data);
            if (rt_value!=RC_OK) return rt_value;
            pt->dirty = false;
            bf->numWrite++;
        }
        pt = pt->next;
    }
    
    closePageFile(fHandle);
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (bm->numPages<=0) return RC_IM_KEY_NOT_FOUND;
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    
    while (pt->currpage!=page->pageNum)
    {
        pt=pt->next;
        if (pt==bf->head) return RC_READ_NON_EXISTING_PAGE;
    }
    
    pt->dirty = true;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (bm->numPages<=0) return RC_IM_KEY_NOT_FOUND;
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    
    while (pt->currpage!=page->pageNum)
    {
        pt=pt->next;
        if (pt==bf->head) return RC_READ_NON_EXISTING_PAGE;
    }
    
    if (pt->fixCount > 0)
        pt->fixCount--;
    else
        return RC_READ_NON_EXISTING_PAGE;
    
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    //current frame2file
    if (bm->numPages<=0) return RC_IM_KEY_NOT_FOUND;
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    SM_FileHandle *fHandle;
    RC rt_value;
    
    rt_value = openPageFile(bm->pageFile, fHandle);
    if (rt_value!=RC_OK) return RC_FILE_NOT_FOUND;
    
    while (pt->currpage!=page->pageNum)
    {
        pt=pt->next;
        if (pt==bf->head)
        {
            closePageFile(fHandle);
            return RC_READ_NON_EXISTING_PAGE;
        }
    }
    
    rt_value = writeBlock(page->pageNum, fHandle, pt->data);
    if (rt_value!=RC_OK)
    {
        closePageFile(fHandle);
        return RC_FILE_NOT_FOUND;
    }
    
    bf->numWrite++;
    closePageFile(fHandle);
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    if (bm->numPages<=0||pageNum<0) return RC_IM_KEY_NOT_FOUND;
    
    if (alreadyPinned(bm,pageNum)) return RC_OK;
    
    switch (bm->strategy)
    {
        case RS_FIFO:
            return pinFIFO(bm,page,pageNum);
            break;
        case RS_LRU:
            return pinLRU(bm,page,pageNum);
            break;
        case RS_CLOCK:
            return pinCLOCK(bm,page,pageNum);
            break;
        case RS_LFU:
            return pinLFU(bm,page,pageNum);
            break;
        case RS_LRU_K:
            return pinLRUK(bm,page,pageNum);
            break;
        default:
            return RC_IM_KEY_NOT_FOUND;
            break;
    }
    return RC_OK;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    PageNumber *rt = calloc(bm->numPages, sizeof(int)); //need free alpha
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    int i;
    for (i=0; i<bm->numPages; i++)
    {
        if (pt->fixCount>0)
            rt[i]=pt->currpage;
        else
            rt[i]=NO_PAGE;
        pt=pt->next;
    }
    return rt;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    bool *rt = calloc(bm->numPages, sizeof(bool)); //need free beta
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    int i;
    for (i=0; i<bm->numPages; i++)
    {
        if (pt->dirty)
            rt[i]=true;
        pt=pt->next;
    }
    return rt;
}

int *getFixCounts (BM_BufferPool *const bm)
{
    PageNumber *rt = calloc(bm->numPages, sizeof(int)); //need free gamma
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    int i;
    for (i=0; i<bm->numPages; i++)
    {
        rt[i]=pt->fixCount;
        pt=pt->next;
    }
    return rt;
}
int getNumReadIO (BM_BufferPool *const bm)
{
    buffer *bf = bm->mgmtData;
    return bf->numRead;
}
int getNumWriteIO (BM_BufferPool *const bm)
{
    buffer *bf = bm->mgmtData;
    return bf->numWrite;
}

int main()
{
    printf("helloworld");
}
