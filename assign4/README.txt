~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Assignment-3: 

	The assignment is to implement a simple record manager that allows navigation through records, and inserting and deleting records. All rights reserved.

* Created by:
	Yinan Zhang 

* Cooperated with:
	Xiao Wang
	Pengyu Guo

* Included files:

	README.txt        dberror.c         makefile          storage_mgr.h
	buffer_mgr.c      dberror.h         record_mgr.c      tables.h
	buffer_mgr.h      dt.h              record_mgr.h      test_assign3_1.c
	buffer_mgr_stat.c expr.c            rm_serializer.c   test_expr.c
	buffer_mgr_stat.h expr.h            storage_mgr.c     test_helper.h


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Installation instruction:

	0. Log in to server and cd to the directory of the code:

	1. Run “make clean” to clean the compiled files, executable files and log files

	2. Run “make” command to compile the code

	3. Run test_assign3_1 

	4. View the result of the test_assign1 file:

	5. If you want to re-run the executable files, start from step 3. If you want to re-compile the files, start from step 1.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Function Description:

1) initRecordManager (void *mgmtData)

	initialize tableData


2) shutdownRecordManager():

	free resources


3) createTable(char *name, Schema *schema):

	create table information and put them in the first page

 
4) openTable (RM_TableData *rel, char *name):
 
	initialize buffer pool
 

5) closeTable (RM_TableData *rel):
 
	shutdown buffer pool
 

6) deleteTable (char *name):
 
	delete the table file
 

7) getNumTuples (RM_TableData *rel):
 
	count the number of tuples in the table


8) insertRecord (RM_TableData *rel, Record *record):
 
	insert the record into table

 
9) deleteRecord (RM_TableData *rel, RID id):
 
	delete the record from table 
 

10) updateRecord (RM_TableData *rel, Record *record):
 
	update the record from table 
 

11) getRecord (RM_TableData *rel, RID id, Record *record):
 
	find the desired record


12) startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond):
 
	initialize scan handle
 

13) next (RM_ScanHandle *scan, Record *record):

	find next
 

14) closeScan (RM_ScanHandle *scan):
 
	close and free the scan handle 


15) getRecordSize (Schema *schema):
	
	return the length needed for this schema
 

16) createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, 
	int keySize, int *keys):
 
	create new schema
 

17) freeSchema (Schema *schema):
 
	free the schema
 

18) createRecord (Record **record, Schema *schema):
 
	create the record with the give schema
 

19) freeRecord (Record *record):
 
	free the record


23) getAttr (Record *record, Schema *schema, int attrNum, Value **value):
 
	get the schema format of the record
 

24) setAttr (Record *record, Schema *schema, int attrNum, Value *value):
 
	set the attributes into a schema format


25) doRecord (Record *record):
	
	serialize the record and put it in the page


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extra Credits:

1) TIDs and tombstones: 

This program keeps track of the total inserted record number (as a TID).
We set '0\' as a tomestone in the place of deleted records.

2) Check primary key constraints:

This program uses a simple hash table to check primary key constraints.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
