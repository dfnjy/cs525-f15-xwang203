#include <string.h>
#include <stdlib.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "test_helper.h"
//#include "rm_serializer.c"


typedef struct RM_tableData_mgmtData{
    int numPages;//the number of pages of the file
    int numRecords;//number of tuples in the table
    int numRecordsPerPage;//total number of records could in one page
    int numInsert;//number of tuples that inserted in the file
    BM_BufferPool *bm;//buffer pool of buffer manage
}RM_tableData_mgmtData;

typedef struct RM_ScanData_mgmtData{
    int numScan;//number of tuple be scanned
    RID nowRID;//the RID of the tuple that scanned now
    Expr *cond;    //select condition of the record
}RM_ScanData_mgmtData;

RM_TableData *tableData = NULL;

RC doRecord (Record *record)
{
    if (record == NULL) return RC_RM_UNKOWN_DATATYPE;
    
    RC rc;
    int pageNum = record->id.page;
    int slot = record->id.slot;
    BM_BufferPool *bm = ((RM_tableData_mgmtData *)tableData->mgmtData)->bm;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    int offslot = getRecordSize(tableData->schema);
    
    page->data = (char *)malloc(PAGE_SIZE);
    
    rc = pinPage(bm, page, pageNum);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    page->data += slot * (offslot);
    memcpy(page->data, record->data, offslot);
    page->data -= slot * (offslot);
    
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
}//doRecord

// -------------------------table and manager

RC initRecordManager (void *mgmtData)
{
    tableData = (RM_TableData *) malloc (sizeof(RM_TableData));
    tableData->mgmtData = mgmtData;
    return RC_OK;
}//initRecordManager

RC shutdownRecordManager ()
{
    if (tableData->schema != NULL){
        free(tableData->schema);
        tableData->schema = NULL;
    }//in case
    free(tableData->mgmtData);
    tableData->mgmtData = NULL;
    free(tableData);
    tableData = NULL;
    
    return RC_OK;
}//shutdownRecordManager

RC createTable (char *name, Schema *schema)
{
    if (name == NULL) return RC_FILE_NOT_FOUND;
    if (schema == NULL) return RC_RM_UNKOWN_DATATYPE;
    
    RC rc;
    tableData->name = name;
    tableData->schema = schema;
    RM_tableData_mgmtData *tableMgm;
    BM_BufferPool *bm;
    
    tableMgm = ((RM_tableData_mgmtData *) malloc (sizeof(RM_tableData_mgmtData)));
    tableMgm->numPages = 0;
    tableMgm->numRecords = 0;
    tableMgm->numRecordsPerPage = 0;
    tableMgm->numInsert = 0;
    
    char *firstPage = (char *)malloc(PAGE_SIZE);
    memcpy(firstPage, &tableMgm->numPages, sizeof(int));
    firstPage += sizeof(int);
    memcpy(firstPage, &tableMgm->numRecords, sizeof(int));
    firstPage += sizeof(int);
    memcpy(firstPage, &tableMgm->numRecordsPerPage, sizeof(int));
    firstPage += sizeof(int);
    memcpy(firstPage, &tableMgm->numInsert, sizeof(int));
    firstPage += sizeof(int);
    //backtracking
    firstPage -= 4*sizeof(int);
    strcat(firstPage, serializeSchema(schema));
    
    rc = createPageFile(tableData->name);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    bm = MAKE_POOL();
    bm->mgmtData = malloc(sizeof(buffer));
    rc = openPageFile(tableData->name, &bm->fH);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    if (0 == getBlockPos(&bm->fH)){
        rc = writeCurrentBlock(&bm->fH, firstPage);
    }else{
        rc = writeBlock(0, &bm->fH, firstPage);
    }
    if (rc != RC_OK)
    {
        return rc;
    }
    
    free(firstPage);
    firstPage = NULL;
    
    tableMgm->numPages = 0;
    tableMgm->bm = bm;
    tableData->mgmtData = tableMgm;
    return RC_OK;
}//createTable

RC openTable (RM_TableData *rel, char *name)
{
    if (name == NULL) return RC_FILE_NOT_FOUND;
    if (tableData == NULL){
        return RC_RM_UNKOWN_DATATYPE;
    }
    RC rc;
    rc = initBufferPool(((RM_tableData_mgmtData *)tableData->mgmtData)->bm, name, 10000, RS_CLOCK, NULL);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    *rel = *tableData;
    return RC_OK;
}

RC closeTable (RM_TableData *rel)
{
    if (tableData == NULL) return RC_RM_UNKOWN_DATATYPE;
    RC rc;
    rc = shutdownBufferPool(((RM_tableData_mgmtData *)tableData->mgmtData)->bm);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    rel->name = NULL;
    return RC_OK;
}

RC deleteTable (char *name)
{
    if (name == NULL) return RC_FILE_NOT_FOUND;
    RC rc;
    rc = destroyPageFile(name);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    return RC_OK;
}

int getNumTuples (RM_TableData *rel)
{
    return ((RM_tableData_mgmtData *)tableData->mgmtData)->numRecords;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{
    if (rel == NULL)
    {
        return -1;
    }
    
    if (record == NULL)
    {
        return -1;
    }
    RC rc;
    int offslot = getRecordSize(tableData->schema);
    RM_tableData_mgmtData *tableMgm = (RM_tableData_mgmtData *)tableData->mgmtData;
    //to calculate the number of tuples per page.
    tableMgm->numRecordsPerPage = PAGE_SIZE/offslot;
    tableMgm->numRecords += 1;
    tableMgm->numInsert += 1;
    
    int slot = (tableMgm->numInsert)%(tableMgm->numRecordsPerPage);
    //record insert into the new file
    if (slot == 1) tableMgm->numPages += 1;
    record->id.page = tableMgm->numPages;
    //record insert into free space of the current file
    if (slot == 0) record->id.slot = tableMgm->numRecordsPerPage - 1;
    else record->id.slot = slot - 1;
    
    rc = doRecord(record);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    tableData->mgmtData = tableMgm;
    *rel = *tableData;
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id)
{
    if (rel == NULL)
    {
        return -1;
    }
    int pageNum = id.page;
    int slot = id.slot;
    RC rc;
    int offslot = getRecordSize(tableData->schema);
    RM_tableData_mgmtData *tableMgm = ((RM_tableData_mgmtData *)tableData->mgmtData);
    BM_BufferPool *bm = tableMgm->bm;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = (char *)malloc(PAGE_SIZE);
    
    rc = pinPage(bm, page, pageNum);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    page->data += (offslot)*slot;
    memset(page->data, 0, offslot);
    page->data -= (offslot)*slot;
    
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
    
    tableMgm->numRecords -= 1;
    
    tableData->mgmtData = tableMgm;
    *rel = *tableData;
    return RC_OK;
}


RC updateRecord (RM_TableData *rel, Record *record)
{
    RC rc;
    if (rel == NULL)
    {
        return -1;
    }
    
    if (record == NULL)
    {
        return -1;
    }
    rc = doRecord(record);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    *rel = *tableData;
    return RC_OK;
}


RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    RC rc;
    if ((tableData == NULL)||(record == NULL))
        return RC_RM_UNKOWN_DATATYPE;
    int pageNum = id.page;
    int slot = id.slot;
    record->id = id;
    int offslot = getRecordSize(tableData->schema);
    RM_tableData_mgmtData *temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    
    BM_BufferPool *bm = temp->bm;
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    
    rc = pinPage(bm, page, pageNum);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    page->data += (offslot)*slot;
    memcpy(record->data, page->data, offslot);
    page->data -= (offslot)*slot;
    
    rc = unpinPage(bm, page);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    return RC_OK;
}


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if ((rel == NULL)||(scan == NULL)||(cond == NULL)) return RC_RM_UNKOWN_DATATYPE;
    RM_ScanData_mgmtData *ScanMgm = ((RM_ScanData_mgmtData *) malloc (sizeof(RM_ScanData_mgmtData)));
    ScanMgm->numScan = 0;
    ScanMgm->cond = cond;
    ScanMgm->nowRID.page = 1;
    ScanMgm->nowRID.slot = 0;
    scan->mgmtData = ScanMgm;
    scan->rel = rel;
    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    if ((scan == NULL)||(record == NULL)) return RC_RM_UNKOWN_DATATYPE;
    RC rc;
    RM_ScanData_mgmtData *ScanMgm = (RM_ScanData_mgmtData *)scan->mgmtData;
    //if the count number is all records number
    RM_tableData_mgmtData *tableMgm = (RM_tableData_mgmtData *)tableData->mgmtData;
    
    Value *res = ((Value *) malloc (sizeof(Value)));
    
    res->v.boolV = FALSE;
    while(!res->v.boolV){
        if (ScanMgm->numScan == tableMgm->numRecords) return RC_RM_NO_MORE_TUPLES;
        rc = getRecord (tableData, ScanMgm->nowRID, record);
        if (rc != RC_OK)
        {
            return rc;
        }
        
        rc = evalExpr (record, tableData->schema, ScanMgm->cond, &res);
        ScanMgm->numScan += 1;
        ScanMgm->nowRID.slot += 1;
        if (ScanMgm->nowRID.slot == tableMgm->numRecordsPerPage){
            ScanMgm->nowRID.page += 1;
            ScanMgm->nowRID.slot = 0;
        }
        
    }
    scan->mgmtData = ScanMgm;
    return RC_OK;
}

RC closeScan (RM_ScanHandle *scan)
{
    if (scan == NULL) return RC_RM_UNKOWN_DATATYPE;
    free(scan->mgmtData);
    scan->mgmtData = NULL;
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema)
{
    if (schema == NULL)
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
                recSize += schema->typeLength[i]+1;
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

static Schema *mallocSchema(int numAttr, int keySize)
{
    Schema *SCHEMA;
    
    // allocate memory to Schema
    SCHEMA = (Schema *)malloc(sizeof(Schema));
    SCHEMA->numAttr = numAttr;
    SCHEMA->attrNames = (char **)malloc(sizeof(char*) * numAttr);
    SCHEMA->typeLength = (int *)malloc(sizeof(int) * numAttr);
    SCHEMA->dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
    SCHEMA->keyAttrs = (int *)malloc(sizeof(int) * keySize);
    SCHEMA->keySize = keySize;
    
    for(int i = 0; i < numAttr; i++)
    {
        SCHEMA->attrNames[i] = (char *) malloc(sizeof(char *));
    }
    
    return SCHEMA;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = mallocSchema(numAttr, keySize);
    schema->numAttr = numAttr;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    for (int i = 0; i< numAttr; i++)
    {
        strcpy(schema->attrNames[i], attrNames[i]);
    }
    return schema;
}


RC freeSchema (Schema *schema)
{
    if (schema == NULL)
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

RC createRecord (Record **record, Schema *schema)
{
    
    if (schema == NULL) return RC_RM_SCHEMA_NOT_FOUND;
    *record = ((Record *) malloc (sizeof(Record)));
    (*record)->data = (char *)malloc(getRecordSize(schema));
    
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    
    return RC_OK;
}


RC freeRecord (Record *record)
{
    
    if (record == NULL) return -1;
    
    free(record->data);
    record->data = NULL;
    free(record);
    record = NULL;
    
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    if (record == NULL) return -1;
    if (schema == NULL) return -1;
    
    RC rc;
    char *recordData = record->data;
    int offattr = 0;
    value[0] = ((Value *) malloc (sizeof(Value)));
    
    rc = attrOffset(schema, attrNum, &offattr);
    recordData += offattr;
    switch(schema->dataTypes[attrNum]){
        case DT_INT:
            memcpy(&(value[0]->v.intV), recordData, sizeof(int));
            break;
        case DT_STRING:
            value[0]->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
            memcpy((value[0]->v.stringV), recordData, schema->typeLength[attrNum] + 1);
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
    recordData -= offattr;
    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    RC rc;
    if (record == NULL) return -1;
    if (schema == NULL) return -1;
    
    if (attrNum < 0 || attrNum >= schema->numAttr) return -1;
    
    char *recordData = record->data;
    int offattr = 0;
    rc = attrOffset(schema, attrNum, &offattr);
    recordData += offattr;
    schema->dataTypes[attrNum] = (value)->dt;
    switch((value)->dt){
        case DT_INT:
            memcpy(recordData, &((value)->v.intV), sizeof(int));
            break;
        case DT_STRING:
            memcpy(recordData, ((value)->v.stringV), schema->typeLength[attrNum] + 1);
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
    recordData -= offattr;
    
    return RC_OK;
}

