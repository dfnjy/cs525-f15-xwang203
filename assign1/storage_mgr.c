#include "storage_mgr.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


/* manipulating page files */
RC createPageFile (char *fileName)
{
    FILE *pFile = fopen(fileName,"w");
    char str[PAGE_SIZE];
    memset(str,'\0',PAGE_SIZE); //generate a page full of '\0'
    int total_pages=0; //total pages to write at the beginning
    fprintf(pFile,"%d\n",total_pages);
    fwrite(str, sizeof(char), PAGE_SIZE, pFile);
    fclose(pFile);
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
    
    fHandle->fileName= NULL;
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

int main(void)
{
    int a=sizeof(char);
    printf("%d",a);
}
