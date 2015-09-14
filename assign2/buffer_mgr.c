#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>
#include <stdlib.h>

typedef struct frame{
    int currpage; //the corresponding page in the file
    bool dirty;
    int fixCount;
    char data[PAGE_SIZE];
    bool refbit; //true=1 false=0 for clock
    struct frame *next;
    struct frame *prev;
}frame;

typedef struct statlist{
    frame *fpt; //frame pt
    struct statlist *next;
}statlist;

typedef struct buffer{ //use as a class
    int numFrames; // number of frames in the frame list
    int numRead; //for readIO
    int numWrite; //for writeIO
    void *stratData; //sizeof(void)=8 siszeof(int)=4; malloc cheating here
    //int pinnNum; //pinned frames
    frame *head;
    frame *tail;
    frame *pointer; //special purposes;init as bfhead;clock used
    statlist *stathead; //statistics functions have to follow true sequence -.-|
}buffer;

/* Custom Functions*/

frame *alreadyPinned(BM_BufferPool *const bm, const PageNumber pageNum)
/*Check if pageNum-th frame was already pinned. If so, increase fixCount.
 Return frame pointer if found, or NULL*/
{
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    do
    {
        if (pt->currpage==pageNum)
        {
            pt->fixCount++; //warning: cheating
            return pt;
        }
        pt=pt->next;
    }while (pt!=bf->head);
    return NULL;// == return false
}

int pinThispage(BM_BufferPool *const bm, frame *pt, PageNumber pageNum)
/*pin page pointed by pt with pageNum-th page. If do not have, create one*/
{
    buffer *bf = bm->mgmtData;
    SM_FileHandle fHandle; //cheating: should malloc then free
    RC rt_value = openPageFile(bm->pageFile, &fHandle);
    if (rt_value!=RC_OK) return rt_value;
    rt_value = ensureCapacity(pageNum, &fHandle); //你不说谁知道啊
    if (rt_value!=RC_OK) return rt_value;
    
    if (pt->dirty)
    {
        rt_value = writeBlock(pt->currpage, &fHandle, pt->data);
        if (rt_value!=RC_OK) return rt_value;
        pt->dirty = false;
        bf->numWrite++;
    }
    
    rt_value = readBlock(pageNum, &fHandle, pt->data);
    if (rt_value!=RC_OK) return rt_value;
    bf->numRead++;
    pt->currpage = pageNum;
    pt->fixCount++;
    closePageFile(&fHandle);
    
    return 0;
}

/* Pinning Functions*/

RC pinFIFO (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum, bool fromLRU)
/*pin the first avaliable frame, then move the frame to tail. Noticed that it is circular queue.
 If was called by LRU then no need to check pinned*/
{
    if (!fromLRU)
        if (alreadyPinned(bm,pageNum)) return RC_OK;
    //load into memory using FIFO
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    //find first avaliable frame
    bool notfind = true;
    do
    {
        if (pt->fixCount == 0)
        {
            notfind = false;
            break;
        }
        pt = pt->next;
    }while (pt!=bf->head);
    
    if (notfind)
        return RC_IM_NO_MORE_ENTRIES; //no avaliable frame
    
    RC rt_value = pinThispage(bm, pt, pageNum);
    if (rt_value!=RC_OK) return rt_value;
    
    page->pageNum = pageNum;
    page->data = pt->data;
    
    //change lists
    if (pt == bf->head)
        bf->head = pt->next;
    pt->prev->next = pt->next;
    pt->next->prev = pt->prev;
    pt->prev = bf->tail;
    bf->tail->next = pt;
    bf->tail = pt;
    pt->next = bf->head;
    bf->head->prev = pt;
    
    return RC_OK;
}

RC pinLRU (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
/*If not pinned then same as FIFO. If pinned then move to tail.*/
{
    frame *pt = alreadyPinned(bm,pageNum);
    if (pt)
    {
        //change priority
        buffer *bf = bm->mgmtData;
        if (pt == bf->head)
            bf->head = pt->next;
        pt->prev->next = pt->next;
        pt->next->prev = pt->prev;
        pt->prev = bf->tail;
        bf->tail->next = pt;
        bf->tail = pt;
        pt->next = bf->head;
        bf->head->prev = pt;
        
        page->pageNum = pageNum;
        page->data = pt->data;
    }
    else
        return pinFIFO(bm, page, pageNum,true);
    return RC_OK;
}

RC pinCLOCK (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
/*use pointer to scan. No need to reorder queue*/
{
    if (alreadyPinned(bm,pageNum)) return RC_OK;
    buffer *bf = bm->mgmtData;
    frame *pt = bf->pointer->next;
    bool notfind = true;
    
    while (pt!=bf->pointer)
    {
        if (pt->fixCount == 0)
        {
            if (!pt->refbit) //refbit = 0
            {
                notfind = false;
                break;
            }
            pt->refbit = false; //on the way set all bits to 0
        }
        pt = pt->next;
    };
    
    if (notfind)
        return RC_IM_NO_MORE_ENTRIES; //no avaliable frame
    
    RC rt_value = pinThispage(bm, pt, pageNum);
    if (rt_value!=RC_OK) return rt_value;
    
    bf->pointer = pt;
    page->pageNum = pageNum;
    page->data = pt->data;
    
    return RC_OK;
}

RC pinLRUK (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    //看倒数第k次操作谁最早就换谁
    return RC_OK;
}

/* Assignment Functions*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
//initialization: create page frames using circular list; init bm;
{
    //error check
    if (numPages<=0) //input check
        return RC_WRITE_FAILED;
    //init bf:bookkeeping data
    buffer *bf = malloc(sizeof(buffer));
    
    if (bf==NULL) return RC_WRITE_FAILED;
    bf->numFrames = numPages;
    bf->stratData = stratData;
    bf->numRead = 0;
    bf->numWrite = 0;
    //create list
    int i;
    frame *phead = malloc(sizeof(frame));
    statlist *shead = malloc(sizeof(statlist));
    if (phead==NULL) return RC_WRITE_FAILED;
    phead->currpage=NO_PAGE;
    phead->refbit=false;
    phead->dirty=false;
    phead->fixCount=0;
    memset(phead->data,'\0',PAGE_SIZE);
    shead->fpt = phead;
    
    bf->head = phead;
    bf->stathead = shead;
    
    for (i=1; i<numPages; i++) { //i start from 1
        frame *pnew = malloc(sizeof(frame));
        statlist *snew = malloc(sizeof(statlist));
        if (pnew==NULL) return RC_WRITE_FAILED;
        pnew->currpage=NO_PAGE;
        pnew->dirty=false;
        pnew->refbit=false;
        pnew->fixCount=0;
        memset(pnew->data,'\0',PAGE_SIZE);
        
        snew->fpt = pnew;
        shead->next = snew;
        shead = snew;
        
        phead->next=pnew;
        pnew->prev=phead;
        phead=pnew;
    }
    shead->next = NULL;
    bf->tail = phead;
    bf->pointer = bf->head;
    
    //circular list for clock
    bf->tail->next = bf->head;
    bf->head->prev = bf->tail;
    
    //init bm
    bm->numPages = numPages;
    bm->pageFile = (char *)pageFileName;
    bm->strategy = strategy;
    bm->mgmtData = bf;
    
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    //write dirty pages back to disk
    RC rt_value = forceFlushPool(bm);
    if (rt_value!=RC_OK) return rt_value;
    //free up resources
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    
    while (pt!=bf->tail)
    {
        pt = pt->next;
        free(bf->head);
        bf->head = pt;
    }
    free(bf->tail);
    free(bf);
    
    bm->numPages = 0;
    bm->pageFile = NULL;
    bm->mgmtData = NULL;
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    //write all dirty pages (fix count 0) to disk
    //do pinned check
    buffer *bf = bm->mgmtData;
    
    SM_FileHandle fHandle;
    RC rt_value = openPageFile(bm->pageFile, &fHandle);
    if (rt_value!=RC_OK) return rt_value;

    frame *pt = bf->head;
    do
    {
        if (pt->dirty)
        {
            rt_value = writeBlock(pt->currpage, &fHandle, pt->data);
            if (rt_value!=RC_OK) return rt_value;
            pt->dirty = false;
            bf->numWrite++;
        }
        pt = pt->next;
    }while (pt!=bf->head);
    
    closePageFile(&fHandle);
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    
    while (pt->currpage!=page->pageNum)
    {
        pt=pt->next;
        if (pt==bf->head)
            return RC_READ_NON_EXISTING_PAGE;
    }
    
    pt->dirty = true;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
//fixcount--
{
    buffer *bf = bm->mgmtData;
    frame *pt = bf->head;
    
    while (pt->currpage!=page->pageNum)
    {
        pt=pt->next;
        if (pt==bf->head)
            return RC_READ_NON_EXISTING_PAGE;
    }
    
    if (pt->fixCount > 0)
    {
        pt->fixCount--;
        if (pt->fixCount == 0)
            pt->refbit = false;
    }
    else
        return RC_READ_NON_EXISTING_PAGE;
    /*
    //debugging para
    if (pt->dirty)
    {
        forcePage(bm, page);
        pt->dirty = false;
    }
    pt->currpage=NO_PAGE;
    memset(pt->data,'\0',PAGE_SIZE);
    */
    
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    //current frame2file
    buffer *bf = bm->mgmtData;
    SM_FileHandle fHandle;
    RC rt_value;
    
    rt_value = openPageFile(bm->pageFile, &fHandle);
    if (rt_value!=RC_OK) return RC_FILE_NOT_FOUND;
    
    rt_value = writeBlock(page->pageNum, &fHandle, page->data);
    if (rt_value!=RC_OK)
    {
        closePageFile(&fHandle);
        return RC_FILE_NOT_FOUND;
    }
    
    bf->numWrite++;
    closePageFile(&fHandle);
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    if (pageNum<0) return RC_IM_KEY_NOT_FOUND;
    
    switch (bm->strategy)
    {
        case RS_FIFO:
            return pinFIFO(bm,page,pageNum,false);
            break;
        case RS_LRU:
            return pinLRU(bm,page,pageNum);
            break;
        case RS_CLOCK:
            return pinCLOCK(bm,page,pageNum);
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
    statlist *spt = bf->stathead;
    int i;
    for (i=0; i<bm->numPages; i++)
    {
        rt[i]=spt->fpt->currpage;
        spt=spt->next;
    }
    return rt;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    bool *rt = calloc(bm->numPages, sizeof(bool)); //need free beta
    buffer *bf = bm->mgmtData;
    statlist *spt = bf->stathead;
    int i;
    for (i=0; i<bm->numPages; i++)
    {
        if (spt->fpt->dirty)
            rt[i]=true;
        spt=spt->next;
    }
    return rt;
}

int *getFixCounts (BM_BufferPool *const bm)
{
    PageNumber *rt = calloc(bm->numPages, sizeof(int)); //need free gamma
    buffer *bf = bm->mgmtData;
    statlist *spt = bf->stathead;
    int i;
    for (i=0; i<bm->numPages; i++)
    {
        rt[i]=spt->fpt->fixCount;
        spt=spt->next;
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
/*
int main()
{
    int *a=malloc(sizeof(char));
    printf("%d",*a);
    free(a);
}
*/