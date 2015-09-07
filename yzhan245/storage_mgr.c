//
//  storage_mgr.c
//  assign1
//
//  Created by Yinan Zhang on 15/9/1.
//  Copyright (c) 2015年 Yinan Zhang. All rights reserved.
//

#include <stdio.h>
#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
    char *fileName;
    int totalNumPages;
    int curPagePos;
    void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (pageNum>fHandle->totalNumPages || pageNum<0)
        return RC_READ_NON_EXISTING_PAGE;
    if (fHandle->mgmtInfo==NULL)
        return RC_FILE_NOT_FOUND;
    
    int seekSucc = fseek(fHandle->mgmtInfo, 5+pageNum*PAGE_SIZE, SEEK_SET);
    if(seekSucc != 0){
        return RC_SEEK_FAILED;
    }
    fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    //return total number of elements
    
    fHandle->curPagePos=pageNum;
    
    return RC_OK;
    
};
extern int getBlockPos (SM_FileHandle *fHandle){
    return fHandle->curPagePos;
}
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0, fHandle, memPage);
}
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos-1, fHandle, memPage);
}
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos+1, fHandle, memPage);
}
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->totalNumPages-1, fHandle, memPage);
}

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);

#endif

