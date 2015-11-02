#include <string.h>
#include <stdlib.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "test_helper.h"

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
    int numPages;//contains the page numbers of the table file
    int numRecords;//contains the record/tuples numbers of the table
    int numRecordsPerPage;//contains the maximum number of record in each page
    int numInsert;//contains how many record has been inserted into file
    BM_BufferPool *bp;//buffer pool from buffer manage
}RM_tableData_mgmtData;

typedef struct RM_SH_mgmtData{
    int numScan;//contain the number of scanned tuples
    RID nowRID;//contain the rid of record which will be scanned this time
    Expr *cond;    //contain the condition which will select the right record
}RM_SH_mgmtData;

RM_TableData *tableData = NULL;

//--------------------Customized

RC serializeFirstPage(Schema *schema, RM_tableData_mgmtData *mgm){
    char *first = (char *)malloc(PAGE_SIZE);
    memcpy(first, &mgm->numPages, sizeof(int));
    first += sizeof(int);
    memcpy(first, &mgm->numRecords, sizeof(int));
    first += sizeof(int);
    memcpy(first, &mgm->numRecordsPerPage, sizeof(int));
    first += sizeof(int);
    memcpy(first, &mgm->numInsert, sizeof(int));
    first += sizeof(int);
    first -= 4*sizeof(int);
    strcat(first, serializeSchema(schema));
    TEST_CHECK(createPageFile(tableData->name));
    //if((rc = openPageFile(name, mgm->bp->mgmtData)) != RC_OK) return rc;
    if(mgm->bp == NULL){
        mgm->bp = MAKE_POOL();
        mgm->bp->mgmtData = (SM_FileHandle *) malloc (sizeof(SM_FileHandle));
    }
    TEST_CHECK(openPageFile(tableData->name, mgm->bp->mgmtData));
    //write to the first page
    if(0 == getBlockPos(mgm->bp->mgmtData)){
        //rc = writeCurrentBlock(mgm->bp->mgmtData, serializeFirstPage(schema));
        TEST_CHECK(writeCurrentBlock(mgm->bp->mgmtData, first));
    }else{
        //rc = writeBlock(0, mgm->bp->mgmtData, serializeFirstPage(schema));
        TEST_CHECK(writeBlock(0, mgm->bp->mgmtData, first));
    }
    free(first);
    first = NULL;
    return RC_OK;
}

/*
 * RC serialRecord(Record *record):
 *
 * input: record information
 * output: RC, code for the result
 * function: put the one record into the table file
 */

RC serialRecord(Record *record){
    if(record == NULL) return RC_RM_UNKOWN_DATATYPE;
    int pageNum = record->id.page;
    int slot = record->id.slot;
    RM_tableData_mgmtData *temp = ((RM_tableData_mgmtData *)tableData->mgmtData);
    BM_BufferPool *bm = temp->bp;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = (char *)malloc(PAGE_SIZE);
    TEST_CHECK(pinPage(bm, page, pageNum));
    page->data += (getRecordSize(tableData->schema))*slot;
    //record: header: TID and tome stone
    //record: content

    memcpy(page->data, record->data, getRecordSize(tableData->schema));
    //page->data = record->data;
    page->data -= (getRecordSize(tableData->schema))*slot;
    TEST_CHECK(markDirty(bm, page));
    TEST_CHECK(unpinPage(bm, page));
    TEST_CHECK(forcePage(bm, page));
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
    //mgm->actualRecordSize = 0;
    //put the schema into the page-0 of the file
    //page-0 contains the header of table
    //use buffer manager function to create the file
    //precaution: the first page must be large enough to contain the content of
    //schema and other information
    if(sizeof(serializeSchema(schema)) + 4*sizeof(int) > PAGE_SIZE) return RC_IM_N_TO_LAGE;
    //write to the first Page
    //mgm->firstPage =
    TEST_CHECK(serializeFirstPage(schema, mgm));
    mgm->numPages = 0;
    tableData->mgmtData = mgm;
    //rel = tableData;
    return RC_OK;
}//createTable

RC openTable (RM_TableData *rel, char *name)
{
    if(name == NULL) return RC_FILE_NOT_FOUND;
    if(tableData == NULL){
        return RC_RM_UNKOWN_DATATYPE;
    }
    TEST_CHECK(initBufferPool(((RM_tableData_mgmtData *)tableData->mgmtData)->bp, name, 10000, RS_CLOCK, NULL));
    rel = tableData;
    return RC_OK;
}

RC closeTable (RM_TableData *rel)
{
    if(tableData == NULL) return RC_RM_UNKOWN_DATATYPE;
    TEST_CHECK(closePageFile((SM_FileHandle *)(((RM_tableData_mgmtData *)tableData->mgmtData)->bp->mgmtData)));
    rel->name = NULL;
    return RC_OK;
}

RC deleteTable (char *name)
{
    if(name == NULL) return RC_FILE_NOT_FOUND;
    if(tableData->name == name){
        TEST_CHECK(destroyPageFile(name));
    }
    return RC_OK;
}

int getNumTuples (RM_TableData *rel)
{
    return ((RM_tableData_mgmtData *)tableData->mgmtData)->numRecords;
}

// handling records in a table

RC insertRecord (RM_TableData *rel, Record *record){
    if(rel == NULL)
        {
            return -1;
        }
    //if(tableData->mgmtData == NULL) return RC_RM_NO_TABLE_MGMT_DATA;
    if(record == NULL)
        {
            return -1;
        }
    RM_tableData_mgmtData *temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    //calculate the number of tuples in one page.
    //one tuple: TID, tome stone, and record content
    //temp->actualRecordSize = sizeof(record->data);
    temp->numRecordsPerPage = PAGE_SIZE/getRecordSize(tableData->schema);//(temp->actualRecordSize);
    //if the file is new
    //else if the record need to be inserted into free space
    temp->numRecords += 1;
    temp->numInsert += 1;
    int slot = (temp->numInsert)%(temp->numRecordsPerPage);
    switch (slot){
                case 0: {RID id_=record->id;
                    id_.slot = temp->numRecordsPerPage - 1;
                    record->id = id_;
                }
                    break;
                case 1: temp->numPages += 1; break;
                default: {RID id_ = record->id;
                    id_.slot = slot - 1;
                    record->id = id_;
                }
                            }
    RID id_ = record->id;
    id_.page = temp->numPages;
    record->id = id_;
    RC check_serial = serialRecord(record);
    TEST_CHECK(check_serial);
    tableData->mgmtData = temp;
    rel = tableData;
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){
    if(rel == NULL)
        {
            return -1;
                }
    //int pageNum = id.page;
    //int slot = id.slot;
    RM_tableData_mgmtData *temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = (char *)malloc(PAGE_SIZE);
    //rc作用？
    //int rc = RC_OK;
    //printf("%p\n\n", &page);
    RC check_pin = pinPage(temp->bp, page, id.page);
    TEST_CHECK(check_pin);
    page->data += (getRecordSize(tableData->schema))*(id.slot);
    //record: header: TID and tome stone
    //record: content
    //
    id.slot = -1;
    memset(page->data, 0, getRecordSize(tableData->schema));
    page->data -= (getRecordSize(tableData->schema))*(id.slot);
    temp->numRecords -= 1;
    RC check_markDirty = markDirty(temp->bp, page);
    RC check_unpin = unpinPage(temp->bp, page);
    RC check_forcePage = forcePage(temp->bp, page);
    TEST_CHECK(check_markDirty);
    TEST_CHECK(check_unpin);
    TEST_CHECK(check_forcePage);
    tableData->mgmtData = temp;
    rel = tableData;
    return RC_OK;
}



RC updateRecord (RM_TableData *rel, Record *record){
    if(rel == NULL)
        {
            return -1;
        }
    //if(rel->mgmtData == NULL) return RC_RM_NO_TABLE_MGMT_DATA;
    if(record == NULL)
        {
            return -1;
        }
    RC check_serial = serialRecord(record);
    TEST_CHECK(check_serial);
    rel = tableData;
    return RC_OK;
}



RC getRecord (RM_TableData *rel, RID id, Record *record){
    if(tableData == NULL)
        {
            return -1;
                }
    if(record == NULL)
        {
            return -1;
        }
    record->id = id;
    RM_tableData_mgmtData *temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    //if(temp->bp == NULL) return RC_RM_NO_TABLE_MGMT_DATA;
    BM_BufferPool *bm = temp->bp;
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    //rc的作用？
    //int rc = RC_OK;
    RC check_pin = pinPage(bm, page, id.page);
    page->data += (getRecordSize(tableData->schema))*(id.slot);
    memcpy(record->data, page->data, getRecordSize(tableData->schema));
    //record->data = page->data;
    TEST_CHECK(check_pin);
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
    //must be tableData;
    RM_SH_mgmtData *mgm = (RM_SH_mgmtData *)scan->mgmtData;
    //if the count number is all records number
    RM_tableData_mgmtData *mgmTable = (RM_tableData_mgmtData *)tableData->mgmtData;
    //printf("%d\n", mgm->numScan);
    //printf("%d\n", mgmTable->numRecords);
    Value *res = MAKE_CVALUE();
    //rec->data = (char *)malloc(getRecordSize(tableData->schema));
    res->v.boolV = FALSE;
    while(!res->v.boolV){
        //printf("######\n");
        if(mgm->numScan == mgmTable->numRecords) return RC_RM_NO_MORE_TUPLES;
        TEST_CHECK(getRecord (tableData, mgm->nowRID, record));
        rc = evalExpr (record, tableData->schema, mgm->cond, &res);
        mgm->numScan += 1;
        mgm->nowRID.slot += 1;
        if(mgm->nowRID.slot == mgmTable->numRecordsPerPage){
            mgm->nowRID.page += 1;
            mgm->nowRID.slot = 0;
        }
        //printf("%d\n", res->v.boolV);
    }
    //record = rec;
    //record->data = rec->data;
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

/*
 *  MALLOC_SCHEMA:Alloc memory to Schema
 */
static Schema *mallocSchema(int numAttr, int keySize){
    Schema *SCHEMA;
    int i;
    // alloc memory to Schema
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
    /*
     free(schema->attrNames);
     free(schema->dataTypes);
     free(schema->typeLength);
     free(schema->keyAttrs);
      */
    return RC_OK;
}


// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema)
{
    if(schema == NULL) return RC_RM_SCHEMA_NOT_FOUND;
    Record *r = (Record *) malloc(sizeof(Record));
    (*record)->data = (char *)malloc(getRecordSize(schema));
    r->id.page = -1;
    r->id.slot = -1;
    return RC_OK;
}
RC freeRecord (Record *record){
    free(record->data);
    record->data = NULL;
    free(record);
    record = NULL;
    return RC_OK;
}

/*
 * int getoffset(Schema *schema, int attrNum):
 *
 * input: schema, and number of attribute in one record
 * output: integer of the offset of the attribute
 * function: return the offset of the attribute in the record data
 */
int getOffset(Schema *schema, int attrNum){
    int i, offSet = 0;
    for (i = 0; i < schema->numAttr; i++)
    {
        if (schema->dataTypes[i] == DT_INT) {
            offSet += sizeof(int);
        } else if (schema->dataTypes[i] == DT_FLOAT) {
            offSet += sizeof(float);
        } else if (schema->dataTypes[i] == DT_BOOL) {
            offSet += sizeof(bool);
        } else if (schema->dataTypes[i] != DT_STRING) {
            offSet += schema->typeLength[i];
        }
            }
    return offSet;
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    if(schema == NULL) return RC_RM_SCHEMA_NOT_FOUND;
    if(attrNum < 0 || attrNum >= schema->numAttr)
    {
        return RC_RM_WRONG_ATTRNUM;
    }
    char *recordData = record->data;
    int offSet = 0;
    value[0] =  ((Value *) malloc (sizeof(Value)));
    offSet = getOffset(schema, attrNum);
    recordData += offSet;
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
    recordData -= offSet;
    return RC_OK;
}



RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    if(schema == NULL) return RC_RM_SCHEMA_NOT_FOUND;
    if(attrNum < 0 || attrNum >= schema->numAttr)
    {
        return RC_RM_WRONG_ATTRNUM;
    }
    char *recordData = record->data;
    int offSet = 0;
    offSet = getOffset(schema, attrNum);
    recordData += offSet;
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
    recordData -= offSet;
    record->data = recordData;
    return RC_OK;
}


/*
int main()
{
    printf("hello");
}
*/