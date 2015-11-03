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

/*
 * RC serialRecord(Record *record):
 *
 * input: record information
 * output: RC, code for the result
 * function: put the one record into the table file
 */

RC serialRecord(Record *record){
    if(record == NULL) return RC_RM_UNKOWN_DATATYPE;
    
    RC rc;
    int pageNum = record->id.page;
    int slot = record->id.slot;
    RM_tableData_mgmtData *temp = ((RM_tableData_mgmtData *)tableData->mgmtData);
    BM_BufferPool *bm = temp->bp;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = (char *)malloc(PAGE_SIZE);
    
    rc =pinPage(bm, page, pageNum);
    if (rc!=RC_OK)
        return rc;
    
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
    
    //put in page 0
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
    if(name == NULL) return RC_FILE_NOT_FOUND;
    if(tableData == NULL){
        return RC_RM_UNKOWN_DATATYPE;
    }
    TEST_CHECK(initBufferPool(((RM_tableData_mgmtData *)tableData->mgmtData)->bp, name, 10000, RS_CLOCK, NULL));
    *rel = *tableData;
    return RC_OK;
}

RC closeTable (RM_TableData *rel)
{
    if(tableData == NULL) return RC_RM_UNKOWN_DATATYPE;
    RM_tableData_mgmtData * temp = (RM_tableData_mgmtData *)tableData->mgmtData;
    TEST_CHECK(shutdownBufferPool(temp->bp));
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
    temp->numRecordsPerPage = PAGE_SIZE/getRecordSize(tableData->schema)-1;
    //if the file is new
    //else if the record need to be inserted into free space
    temp->numRecords += 1;
    temp->numInsert += 1;
    int slot = (temp->numInsert)%(temp->numRecordsPerPage);
    if(slot == 1) temp->numPages += 1;
    
    record->id.page = temp->numPages;
    if(slot == 0) record->id.slot = temp->numRecordsPerPage - 1;
    else record->id.slot = slot - 1;
    
    TEST_CHECK(serialRecord(record));
    tableData->mgmtData = temp;
    *rel = *tableData;
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){
    if(rel == NULL)
        {
            return -1;
                }
    int pageNum = id.page;
    int slot = id.slot;
    
    RM_tableData_mgmtData *temp = ((RM_tableData_mgmtData *)tableData->mgmtData);
    //BM_BufferPool *bm = temp->bp;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = (char *)malloc(PAGE_SIZE);
    
    //printf("%p\n\n", &page);
    TEST_CHECK(pinPage(temp->bp, page, pageNum));
    
    page->data += (getRecordSize(tableData->schema))*slot;
    
    
    //record: header: TID and tome stone
    //record: content
    //
    slot = -1;
    // memcpy(page->data, &pageNum, sizeof(int));
    // page->data += sizeof(int);
    // memcpy(page->data, &slot, sizeof(int));
    // page->data += sizeof(int);
    memset(page->data, 0, getRecordSize(tableData->schema));
    
    
    page->data -= (getRecordSize(tableData->schema))*slot;
    
    TEST_CHECK(markDirty(temp->bp, page));
    TEST_CHECK(unpinPage(temp->bp, page));
    TEST_CHECK(forcePage(temp->bp, page));
    
    temp->numRecords -= 1;
    
    tableData->mgmtData = temp;
    *rel = *tableData;
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
    TEST_CHECK(serialRecord(record));
    *rel = *tableData;
    return RC_OK;
}



RC getRecord (RM_TableData *rel, RID id, Record *record){
    if ((tableData == NULL)||(record == NULL))
        return RC_RM_UNKOWN_DATATYPE;
    int pageNum = id.page;
    int slot = id.slot;
    record->id = id;
    
    RM_tableData_mgmtData *temp = (RM_tableData_mgmtData *)tableData->mgmtData;

    BM_BufferPool *bm = temp->bp;
    BM_PageHandle *page = MAKE_PAGE_HANDLE();

    TEST_CHECK(pinPage(bm, page, pageNum));
    page->data += (getRecordSize(tableData->schema))*slot;
    
    memcpy(record->data, page->data, getRecordSize(tableData->schema));
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
    Value *res = ((Value *) malloc (sizeof(Value)));
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

/* yinan zhang
// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema)
{
    if(schema == NULL) return RC_RM_SCHEMA_NOT_FOUND;
    *record = MAKE_RECORD();
    (*record)->data = (char *)malloc(getRecordSize(schema));
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    return RC_OK;
}
RC freeRecord (Record *record){
    free(record->data);
    record->data = NULL;
    free(record);
    record = NULL;
    return RC_OK;
}

 *
 * int getoffset(Schema *schema, int attrNum):
 *
 * input: schema, and number of attribute in one record
 * output: integer of the offset of the attribute
 * function: return the offset of the attribute in the record data
 *
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
 */

// dealing with records and attribute values
/*
 * RC createRecord (Record **record, Schema *schema):
 *
 * input: record which contain the new record needed to be returned, schema
 * output: RC, code for the result
 * function: create the record with the information of schema
 */
RC createRecord (Record **record, Schema *schema){
    
    if(schema == NULL) return RC_RM_SCHEMA_NOT_FOUND;
    
    *record = MAKE_RECORD();
    (*record)->data = (char *)malloc(getRecordSize(schema));
    
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    
    return RC_OK;
}


/*
 * RC freeRecord (Record *record):
 *
 * input: record need to be free
 * output: RC, code for the result
 * function: free the space of the record
 */
RC freeRecord (Record *record){
    
    if(record == NULL) return -1;
    
    
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
int getoffset(Schema *schema, int attrNum){
    
    int i, off = 0;
    for(i = 0; i < attrNum; i++){
        switch(schema->dataTypes[i]){
            case DT_INT:
                off += sizeof(int);
                break;
            case DT_STRING:
                off += schema->typeLength[i];
                break;
            case DT_FLOAT:
                off += sizeof(float);
                break;
            case DT_BOOL:
                off += sizeof(bool);
                break;
            default:
                return RC_RM_UNKOWN_DATATYPE;
        }
    }
    return off;
}

/*
 * RC getAttr (Record *record, Schema *schema, int attrNum, Value **value):
 *
 * input: record, schema, number of attribute need to be return, value
 * output: RC, code for the result
 * function: find the right value of the attribute and put it into value
 */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    
    if(record == NULL) return -1;
    if(schema == NULL) return -1;
    
    if(attrNum < 0 || attrNum >= schema->numAttr) return -1;
    
    char *recordData = record->data;
    int off = 0;
    value[0] = ((Value *) malloc (sizeof(Value)));
    
    off = getoffset(schema, attrNum);
    //printf("%d\t%d\n", schema->numAttr, attrNum);
    //temp->dt = ;
    recordData += off;
    switch(schema->dataTypes[attrNum]){
        case DT_INT:
            //printf("abc1\n");
            memcpy(&(value[0]->v.intV), recordData, sizeof(int));
            //temp->v.intV = tempInt;
            //printf("abcc\n");
            break;
        case DT_STRING:
            //printf("abc2\n");
            value[0]->v.stringV = (char *)malloc(sizeof(schema->typeLength[attrNum]));
            memcpy((value[0]->v.stringV), recordData, sizeof(schema->typeLength[attrNum]));
            break;
        case DT_FLOAT:
            //printf("abc3\n");
            memcpy(&(value[0]->v.floatV), recordData, sizeof(float));
            break;
        case DT_BOOL:
            //printf("abc4\n");
            memcpy(&(value[0]->v.boolV), recordData, sizeof(bool));
            break;
            
        default:
            return RC_RM_UNKOWN_DATATYPE;
            
    }
    
    /*value[0] = (Value *)malloc(sizeof(Value));
     value[0] = temp;
     printf("%p\n", &(*value));*/
    value[0]->dt = schema->dataTypes[attrNum];
    recordData -= off;
    return RC_OK;
}

/*
 * RC setAttr (Record *record, Schema *schema, int attrNum, Value *value):
 *
 * input: record, schema, number of attribute need to be return, value
 * output: RC, code for the result
 * function: find the right position of the attribute and set it with new value
 */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    
    if(record == NULL) return -1;
    if(schema == NULL) return -1;
    
    if(attrNum < 0 || attrNum >= schema->numAttr) return -1;
    
    char *recordData = record->data;
    int off = 0;
    
    off = getoffset(schema, attrNum);
    
    recordData += off;
    
    schema->dataTypes[attrNum] = (value)->dt;
    switch((value)->dt){
        case DT_INT:
            memcpy(recordData, &((value)->v.intV), sizeof(int));
            //sprintf(recordData);
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
    //record->data = recordData;
    
    return RC_OK;
}


/*
int main()
{
    printf("hello");
}
*/