#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include<stdlib.h>
#include<string.h>


#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "test_helper.h"

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
    RM_TableData *rel;
    void *mgmtData;
} RM_ScanHandle;

// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan)
{
    
   
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

#endif // RECORD_MGR_H
