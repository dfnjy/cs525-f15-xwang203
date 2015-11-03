#ifndef BUFFER_MGR_STAT_H
#define BUFFER_MGR_STAT_H

#include "buffer_mgr.h"/Users/ziluguo/Desktop/CS525project/cs525-f15-xwang203/assign3/buffer_mgr_stat.c

// debug functions
void printPoolContent (BM_BufferPool *const bm);
void printPageContent (BM_PageHandle *const page);
char *sprintPoolContent (BM_BufferPool *const bm);
char *sprintPageContent (BM_PageHandle *const page);

#endif
