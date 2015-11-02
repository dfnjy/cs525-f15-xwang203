#include "storage_mgr.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void initStorageManager (void){
    //blank
}

/* ========== manipulating page files ============ */
RC createPageFile (char *fileName)
{
    FILE *pFile = fopen(fileName,"w");
    
    SM_PageHandle str = malloc(PAGE_SIZE); //apply for a page using malloc in C
    if (str==NULL)
        return RC_WRITE_FAILED;
    memset(str,'\0',PAGE_SIZE); //fill a page full of '\0'
    
    fprintf(pFile,"%d\n",1); //write total pages=1
    fwrite(str, sizeof(char), PAGE_SIZE, pFile);
    fclose(pFile);
    
    free(str);
    str=NULL;
    return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    FILE *pFile = fopen(fileName,"r+");
    if (!pFile)
        return RC_FILE_NOT_FOUND;
    //write fHandle
    int total_pages;
    fscanf(pFile,"%d\n",&total_pages);
    
    fHandle->fileName=fileName;
    fHandle->totalNumPages=total_pages; //total numbers, not offset
    fHandle->curPagePos=0;  //but this is an offset
    fHandle->mgmtInfo=pFile; //POSIX file descriptor
    
    return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle)
{
    int fail=fclose(fHandle->mgmtInfo); //succeed when 0
    if (fail)
        return RC_FILE_HANDLE_NOT_INIT;
    
    //free(fHandle->fileName);
    //fHandle->fileName= NULL;
    //free(fHandle->mgmtInfo);
    //fHandle->mgmtInfo= NULL;
    return RC_OK;
}

RC destroyPageFile (char *fileName)
{
    int fail=remove(fileName); //succeed=0 error=-1
    if (fail)
        return RC_FILE_NOT_FOUND;
    
    return RC_OK;
}

/* =========== reading blocks from disc =========== */

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (pageNum>fHandle->totalNumPages || pageNum<0)
        return RC_READ_NON_EXISTING_PAGE;
    if (fHandle->mgmtInfo==NULL)
        return RC_FILE_NOT_FOUND;
    
    fseek(fHandle->mgmtInfo, 5+pageNum*PAGE_SIZE, SEEK_SET);
    fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo); //return total number of elements
    fHandle->curPagePos=pageNum;
    
    return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle){
    return fHandle->curPagePos;
}
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0, fHandle, memPage);
}
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos-1, fHandle, memPage);
}
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos+1, fHandle, memPage);
}
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->totalNumPages-1, fHandle, memPage); //convert to offset
}

/* ============ writing blocks to a page file ============ */
//sizeofchar=1 sizeofint=4
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (pageNum > (fHandle->totalNumPages) || pageNum<0)
        return RC_WRITE_FAILED;
    
    int fail = fseek(fHandle->mgmtInfo, 5+pageNum*PAGE_SIZE, SEEK_SET);
    if (fail)
        return RC_WRITE_FAILED;
    
    fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    SM_PageHandle str = (char *) calloc(PAGE_SIZE,1); //apply for a page filled with zero bytes
    //append the page
    
    RC return_value = writeBlock(fHandle->totalNumPages, fHandle, str);
    //if append new, totalNumPages can be used as an offset
    if (return_value!=RC_OK)
    {
        free(str);
        str=NULL;
        return return_value;
    }
    fHandle->curPagePos=fHandle->totalNumPages;
    fHandle->totalNumPages+=1;
    //change total pages in file
    rewind(fHandle->mgmtInfo);//reset file pointer
    fprintf(fHandle->mgmtInfo,"%d\n",fHandle->totalNumPages);
    fseek(fHandle->mgmtInfo,5+(fHandle->curPagePos)*PAGE_SIZE,SEEK_SET); //recover file pointer
    
    free(str);
    str=NULL;
    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
//warning: here's assuming numberOfPages is not an offset
{
    if (fHandle->totalNumPages < numberOfPages)
    {
        int diff = numberOfPages - fHandle->totalNumPages;
        RC return_value;
        int i; //you have to declare i outside of the loop >.>
        for (i=0; i < diff; i++)
        {
            return_value = appendEmptyBlock(fHandle);
            if (return_value!=RC_OK)
                return return_value;
        }
    }
    return RC_OK;
}

