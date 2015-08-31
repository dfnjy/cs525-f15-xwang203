#include "storage_mgr.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


/* manipulating page files */
RC createPageFile (char *fileName)
{
    FILE *pFile = fopen(fileName,"w");
    
    SM_PageHandle str = (char *)malloc(PAGE_SIZE); //apply for a page
    memset(str,'\0',PAGE_SIZE); //fill a page full of '\0'
    
    fprintf(pFile,"%d\n",0); //total_pages=0, total_pages is an offset
    fwrite(str, sizeof(char), PAGE_SIZE, pFile);
    fclose(pFile);
    
    free(str);
    str=NULL;
    return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    FILE *pFile = fopen(fileName,"r");
    if (!pFile)
        return RC_FILE_NOT_FOUND;
    //write fHandle
    int total_pages;
    fscanf(pFile,"%d\n",&total_pages);
    
    fHandle->fileName=fileName;
    fHandle->totalNumPages=total_pages;
    fHandle->curPagePos=0;
    fHandle->mgmtInfo=pFile; //POSIX file descriptor
    
    return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle)
{
    int fail=fclose(fHandle->mgmtInfo); //succeed when 0
    if (fail)
        return RC_FILE_HANDLE_NOT_INIT;
    
    free(fHandle->fileName);
    fHandle->fileName= NULL;
    free(fHandle->mgmtInfo);
    fHandle->mgmtInfo= NULL;
    return RC_OK;
}

RC destroyPageFile (char *fileName)
{
    int fail=remove(fileName); //succeed=0 error=-1
    if (fail)
        return RC_FILE_NOT_FOUND;
    
    return RC_OK;
}

/* writing blocks to a page file */
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
    SM_PageHandle str = (char *)calloc(PAGE_SIZE,1); //apply for a page filled with zero bytes
    //append the page
    fHandle->totalNumPages+=1;
    fHandle->curPagePos=fHandle->totalNumPages;
    RC return_value = writeBlock(fHandle->totalNumPages, fHandle, str);
    if (return_value!=RC_OK)
    {
        free(str);
        str=NULL;
        return return_value;
    }
    //change total_pages
    rewind(fHandle->mgmtInfo);//reset file pointer
    fprintf(fHandle->mgmtInfo,"%d\n",fHandle->totalNumPages);
    fseek(fHandle->mgmtInfo,5+(fHandle->totalNumPages)*PAGE_SIZE,SEEK_SET); //recover file pointer
    
    free(str);
    str=NULL;
    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
//warning: here's assuming numberOfPages is also an offset
{
    if (fHandle->totalNumPages < numberOfPages)
    {
        int diff = numberOfPages - fHandle->totalNumPages;
        RC return_value;
        for (int i=0; i < diff; i++)
        {
            return_value = appendEmptyBlock(fHandle);
            if (return_value!=RC_OK)
                return return_value;
        }
    }
    return RC_OK;
}

int main(void)
{
    printf("%c",'\0');
}
