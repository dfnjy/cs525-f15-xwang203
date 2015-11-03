#include <string.h>
#include <stdlib.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "test_helper.h"
//#include "rm_serializer.c"

#define MAKE_CVALUE()            \
((Value *) malloc (sizeof(Value)))
#define MAKE_RECORD()            \
((Record *) malloc (sizeof(Record)))
#define MAKE_SCHEMA()            \
((Schema *) malloc (sizeof(Schema)))
#define MAKE_TABLE_DATA()				\
((RM_TableData *) malloc (sizeof(RM_TableData)))
#define MAKE_MGMTDATA()            \
((RM_tableData_mgmtData *) malloc (sizeof(RM_tableData_mgmtData)))
#define MAKE_SCAN_MGMTDATA()            \
((RM_SH_mgmtData *) malloc (sizeof(RM_SH_mgmtData)))

typedef struct RM_tableData_mgmtData{
    int numPages;//the number of pages of the file
    int numRecords;//number of tuples in the table
    int numRecordsPerPage;//total number of records could in one page
    int numInsert;//number of tuples that inserted in the file
    BM_BufferPool *bp;//buffer pool of buffer manage
}RM_tableData_mgmtData;

typedef struct RM_SH_mgmtData{
    int numScan;//number of tuple be scanned
    RID nowRID;//the RID of the tuple that scanned now
    Expr *cond;    //select condition of the record
}RM_SH_mgmtData;

RM_TableData *tableData = NULL;

RC serialRecord(Record *record){
    RC rc;
    if(record == NULL) return RC_RM_UNKOWN_DATATYPE;
    
    int pageNum = record->id.page;
    int slot = record->id.slot;
    RM_tableData_mgmtData *temp = ((RM_tableData_mgmtData *)tableData->mgmtData);
    BM_BufferPool *bm = temp->bp;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = (char *)malloc(PAGE_SIZE);
    
    rc = pinPage(bm, page, pageNum);
    if (rc != RC_OK)
    {
        return rc;
    }
    page->data += (getRecordSize(tableData->schema))*slot;
    
    memcpy(page->data, record->data, getRecordSize(tableData->schema));
    
    page->data -= (getRecordSize(tableData->schema))*slot;
    
    rc = markDirty(bm, page);
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = unpinPage(bm, page);
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = forcePage(bm, page);
    if (rc != RC_OK)
    {
        return rc;
    }

    return RC_OK;
}

// -------------------------table and manager

RC initRecordManager (void *mgmtData)
{
    tableData = (RM_TableData *) malloc (sizeof(RM_TableData));
    tableData->mgmtData = mgmtData;
    return RC_OK;
}//initRecordManager

RC shutdownRecordManager ()
{
    if(tableData->schema != NULL){
        free(tableData->schema);
        tableData->schema = NULL;
    }
    free(tableData->mgmtData);
    tableData->mgmtData = NULL;
    free(tableData);
    tableData = NULL;
    return RC_OK;
}//shutdownRecordManager

RC createTable (char *name, Schema *schema)
{
    RC rc;
    if(name == NULL) return RC_FILE_NOT_FOUND;
    if(schema == NULL) return RC_RM_UNKOWN_DATATYPE;
    tableData->name = (char *)malloc(sizeof(name));
    tableData->name = name;
    tableData->schema = MAKE_SCHEMA();
    tableData->schema = schema;
    //create the name file to contain the table
    RM_tableData_mgmtData *mgm;
    mgm = MAKE_MGMTDATA();
    mgm->numPages = 0;
    mgm->numRecords = 0;
    mgm->numRecordsPerPage = 0;
    mgm->numInsert = 0;
    
    /* The header of the table and the Schema are in page 0 of the file
    The first page need to be large enough in order to contain all the needed information */
    if(sizeof(serializeSchema(schema)) + 4*sizeof(int) > PAGE_SIZE)
        return RC_IM_N_TO_LAGE;
    
    char *firstPage = (char *)malloc(PAGE_SIZE);
    memcpy(firstPage, &mgm->numPages, sizeof(int));
    firstPage += sizeof(int);
    memcpy(firstPage, &mgm->numRecords, sizeof(int));
    firstPage += sizeof(int);
    memcpy(firstPage, &mgm->numRecordsPerPage, sizeof(int));
    firstPage += sizeof(int);
    memcpy(firstPage, &mgm->numInsert, sizeof(int));
    firstPage += sizeof(int);
    //backtracking
    firstPage -= 4*sizeof(int);
    
    strcat(firstPage, serializeSchema(schema));
    TEST_CHECK(createPageFile(tableData->name));
    mgm->bp = MAKE_POOL();
    mgm->bp->mgmtData = malloc(sizeof(buffer));
    TEST_CHECK(openPageFile(tableData->name, &mgm->bp->fH));
    if(0 == getBlockPos(&mgm->bp->fH)){
        TEST_CHECK(writeCurrentBlock(&mgm->bp->fH, firstPage));
    }else{
        TEST_CHECK(writeBlock(0, &mgm->bp->fH, firstPage));
    }
    free(firstPage);
    firstPage = NULL;
    
    mgm->numPages = 0;
    tableData->mgmtData = mgm;
    return RC_OK;
}//createTable

RC openTable (RM_TableData *rel, char *name)
{
    RC rc;
    if(name == NULL) return RC_FILE_NOT_FOUND;
    if(tableData == NULL){
        return RC_RM_UNKOWN_DATATYPE;
    }
    rc = initBufferPool(((RM_tableData_mgmtData *)tableData->mgmtData)->bp, name, 10000, RS_CLOCK, NULL);
    if (rc != RC_OK)
    {
        return rc;
    }

    *rel = *tableData;
    return RC_OK;
}

RC closeTable (RM_TableData *rel)
{
    RC rc;
    if(tableData == NULL) return RC_RM_UNKOWN_DATATYPE;
    RM_tableData_mgmtData * temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    rc = shutdownBufferPool(temp->bp);
    if (rc != RC_OK)
    {
        return rc;
    }

    rel->name = NULL;
    return RC_OK;
}

RC deleteTable (char *name)
{
    RC rc;
    if(name == NULL) return RC_FILE_NOT_FOUND;
    if(tableData->name == name){
        rc = destroyPageFile(name);
        if (rc != RC_OK)
        {
            return rc;
        }

    }
    return RC_OK;
}

int getNumTuples (RM_TableData *rel)
{
    return ((RM_tableData_mgmtData *)tableData->mgmtData)->numRecords;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
    RC rc;
    if(rel == NULL)
        {
            return -1;
        }
    
    
    if(record == NULL)
    {
        return -1;
    }
    RM_tableData_mgmtData *temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    //to calculate the number of tuples per page.
    temp->numRecordsPerPage = PAGE_SIZE/getRecordSize(tableData->schema);
    temp->numRecords += 1;
    temp->numInsert += 1;
    int slot = (temp->numInsert)%(temp->numRecordsPerPage);
    //record insert into the new file
    if(slot == 1) temp->numPages += 1;
    record->id.page = temp->numPages;
    //record insert into free space of the current file
    if(slot == 0) record->id.slot = temp->numRecordsPerPage - 1;
    else record->id.slot = slot - 1;
    
    rc = serialRecord(record);
    if (rc != RC_OK)
    {
        return rc;
    }

    tableData->mgmtData = temp;
    *rel = *tableData;
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){
    RC rc;
    if(rel == NULL)
    {
        return -1;
    }
    int pageNum = id.page;
    int slot = id.slot;
    
    RM_tableData_mgmtData *temp = ((RM_tableData_mgmtData *)tableData->mgmtData);
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = (char *)malloc(PAGE_SIZE);
    
    rc = pinPage(temp->bp, page, pageNum);
    if (rc != RC_OK)
    {
        return rc;
    }

    
    page->data += (getRecordSize(tableData->schema))*slot;
    
    
    //record inclueds 1.header: TID,tome stone  2.content
    slot = -1;
    memset(page->data, 0, getRecordSize(tableData->schema));
    
    page->data -= (getRecordSize(tableData->schema))*slot;
    
    rc = markDirty(temp->bp, page);
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = unpinPage(temp->bp, page);
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = forcePage(temp->bp, page);
    if (rc != RC_OK)
    {
        return rc;
    }

    
    temp->numRecords -= 1;
    
    tableData->mgmtData = temp;
    *rel = *tableData;
    return RC_OK;
}



RC updateRecord (RM_TableData *rel, Record *record){
    RC rc;
    if(rel == NULL)
        {
            return -1;
        }
    
    if(record == NULL)
        {
            return -1;
        }
    rc = serialRecord(record);
    if (rc != RC_OK)
        {
            return rc;
        }

    *rel = *tableData;
    return RC_OK;
}



RC getRecord (RM_TableData *rel, RID id, Record *record){
    RC rc;
    if ((tableData == NULL)||(record == NULL))
        return RC_RM_UNKOWN_DATATYPE;
    int pageNum = id.page;
    int slot = id.slot;
    record->id = id;
    
    RM_tableData_mgmtData *temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    
    BM_BufferPool *bm = temp->bp;
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    
    rc = pinPage(bm, page, pageNum);
    if (rc != RC_OK)
         {
            return rc;
         }


    page->data += (getRecordSize(tableData->schema))*slot;
    
    memcpy(record->data, page->data, getRecordSize(tableData->schema));
    rc = unpinPage(bm, page);
    if (rc != RC_OK)
         {
            return rc;
         }

    return RC_OK;
}


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    if ((rel == NULL)||(scan == NULL)||(cond == NULL)) return RC_RM_UNKOWN_DATATYPE;
    RM_SH_mgmtData *mgm = MAKE_SCAN_MGMTDATA();
    mgm->numScan = 0;
    mgm->cond = cond;
    mgm->nowRID.page = 1;
    mgm->nowRID.slot = 0;
    scan->mgmtData = mgm;
    scan->rel = rel;
    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record){
    if ((scan == NULL)||(record == NULL)) return RC_RM_UNKOWN_DATATYPE;
    RC rc;
    RM_SH_mgmtData *mgm = (RM_SH_mgmtData *)scan->mgmtData;
    //if the count number is all records number
    RM_tableData_mgmtData *mgmTable = (RM_tableData_mgmtData *)tableData->mgmtData;

    Value *res = MAKE_CVALUE();

    res->v.boolV = FALSE;
    while(!res->v.boolV){
        if(mgm->numScan == mgmTable->numRecords) return RC_RM_NO_MORE_TUPLES;
        rc = getRecord (tableData, mgm->nowRID, record);
        if (rc != RC_OK)
        {
            return rc;
        }

        rc = evalExpr (record, tableData->schema, mgm->cond, &res);
        mgm->numScan += 1;
        mgm->nowRID.slot += 1;
        if(mgm->nowRID.slot == mgmTable->numRecordsPerPage){
            mgm->nowRID.page += 1;
            mgm->nowRID.slot = 0;
        }
        
    }
    scan->mgmtData = mgm;
    return RC_OK;
}

RC closeScan (RM_ScanHandle *scan){
    if(scan == NULL) return RC_RM_UNKOWN_DATATYPE;
    free(scan->mgmtData);
    scan->mgmtData = NULL;
    return RC_OK;
}


// dealing with schemas
int getRecordSize (Schema *schema)
{
    if(schema == NULL)
    {
        return RC_RM_SCHEMA_NOT_FOUND;
    }
    int recSize = 0;
    int i;
    for(i = 0; i < schema->numAttr; i++)
    {
        switch(schema->dataTypes[i])
        {
            case DT_INT:
                recSize += sizeof(int);
                break;
            case DT_STRING:
                recSize += schema->typeLength[i];
                break;
            case DT_FLOAT:
                recSize += sizeof(float);
                break;
            case DT_BOOL:
                recSize += sizeof(bool);
                break;
            default:
                return RC_RM_UNKOWN_DATATYPE;
        }
    }
    return recSize;
}


static Schema *mallocSchema(int numAttr, int keySize){
    Schema *SCHEMA;
    int i;
    // allocate memory to Schema
    SCHEMA = (Schema *)malloc(sizeof(Schema));
    SCHEMA->numAttr = numAttr;
    SCHEMA->attrNames = (char **)malloc(sizeof(char*) * numAttr);
    SCHEMA->dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
    SCHEMA->typeLength = (int *)malloc(sizeof(int) * numAttr);
    SCHEMA->keySize = keySize;
    SCHEMA->keyAttrs = (int *)malloc(sizeof(int) * keySize);
    for(i = 0; i < numAttr; i++)
    {
        SCHEMA->attrNames[i] = (char *) malloc(sizeof(char *));
    }
    return SCHEMA;
}



Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = mallocSchema(numAttr, keySize);
    int i;
    for (i = 0; i< numAttr; i++)
    {
        strcpy(schema->attrNames[i], attrNames[i]);
    }
    schema->numAttr = numAttr;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    return schema;
}


RC freeSchema (Schema *schema)
{
    if(schema == NULL)
    {
        return RC_RM_SCHEMA_NOT_FOUND;
    }
    free(schema->attrNames);
    schema->attrNames = NULL;
    free(schema->dataTypes);
    schema->dataTypes = NULL;
    free(schema->typeLength);
    schema->typeLength = NULL;
    free(schema->keyAttrs);
    schema->keyAttrs = NULL;
    free(schema);
    schema = NULL;
    
    return RC_OK;
}



RC createRecord (Record **record, Schema *schema){
    
    if(schema == NULL) return RC_RM_SCHEMA_NOT_FOUND;
    
    *record = MAKE_RECORD();
    (*record)->data = (char *)malloc(getRecordSize(schema));
    
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    
    return RC_OK;
}


RC freeRecord (Record *record){
    
    if(record == NULL) return -1;
    
    
    free(record->data);
    record->data = NULL;
    
    
    free(record);
    record = NULL;
    
    return RC_OK;
}



RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    RC rc;
    if(record == NULL) return -1;
    if(schema == NULL) return -1;
    
    if(attrNum < 0 || attrNum >= schema->numAttr) return -1;
    
    char *recordData = record->data;
    int off = 0;
    value[0] = ((Value *) malloc (sizeof(Value)));
    
    rc = attrOffset(schema, attrNum, &off);
    recordData += off;
    switch(schema->dataTypes[attrNum]){
        case DT_INT:
            memcpy(&(value[0]->v.intV), recordData, sizeof(int));
            break;
        case DT_STRING:
            value[0]->v.stringV = (char *)malloc(sizeof(schema->typeLength[attrNum]));
            memcpy((value[0]->v.stringV), recordData, sizeof(schema->typeLength[attrNum]));
            break;
        case DT_FLOAT:
            memcpy(&(value[0]->v.floatV), recordData, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(&(value[0]->v.boolV), recordData, sizeof(bool));
            break;
            
        default:
            return RC_RM_UNKOWN_DATATYPE;
            
    }
    
    value[0]->dt = schema->dataTypes[attrNum];
    recordData -= off;
    return RC_OK;
}



RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    RC rc;
    if(record == NULL) return -1;
    if(schema == NULL) return -1;
    
    if(attrNum < 0 || attrNum >= schema->numAttr) return -1;
    
    char *recordData = record->data;
    int off = 0;
    
    rc = attrOffset(schema, attrNum, &off);
    recordData += off;
    
    schema->dataTypes[attrNum] = (value)->dt;
    switch((value)->dt){
        case DT_INT:
            memcpy(recordData, &((value)->v.intV), sizeof(int));
            break;
        case DT_STRING:
            memcpy(recordData, ((value)->v.stringV), sizeof(schema->typeLength[attrNum]));
            break;
        case DT_FLOAT:
            memcpy(recordData, &((value)->v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(recordData, &((value)->v.boolV), sizeof(bool));
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
    }
    recordData -= off;
    
    return RC_OK;
}

