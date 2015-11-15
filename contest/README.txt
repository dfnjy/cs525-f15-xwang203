~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Assignment-4: 

	The assignment is to implement a B+ tree index. All rights reserved.

* Created by:
	Yinan Zhang 

* Cooperated with:
	Xiao Wang
	Pengyu Guo

* Included files:

	README.txt        buffer_mgr_stat.h makefile          tables.h
	btree_mgr.c       dberror.c         record_mgr.c      test_assign4_1.c
	btree_mgr.h       dberror.h         record_mgr.h      test_expr.c
	buffer_mgr.c      dt.h              rm_serializer.c   test_helper.h
	buffer_mgr.h      expr.c            storage_mgr.c
	buffer_mgr_stat.c expr.h            storage_mgr.h


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Installation instruction:

	0. Log in to server and cd to the directory of the code:

	1. Run “make clean” to clean the compiled files, executable files and log files

	2. Run “make” command to compile the code

	3. Run test_assign4_1 

	4. View the result of the test_assign4 file:

	5. If you want to re-run the executable files, start from step 3. If you want to re-compile the files, start from step 1.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Function Description:

1) initIndexManager (void *mgmtData)

	Initialize index manager.

2) shutdownIndexManager ()

	Close index manager and free resources.

3) createBtree (char *idxId, DataType keyType, int n)

	Apply resources and initialize a B+ tree.

4) openBtree (BTreeHandle **tree, char *idxId)

	Open an existing B+ tree.

5) closeBtree (BTreeHandle *tree)

	Close a B+ tree and free resources.

6) deleteBtree (char *idxId)

	Delete a B+ tree from disk.

7) getNumNodes (BTreeHandle *tree, int *result)

	Get number of nodes from a B+ tree.

8) getNumEntries (BTreeHandle *tree, int *result)

	Get number of entries from a B+ tree.

9) getKeyType (BTreeHandle *tree, DataType *result)

	Get Key Type of a B+ tree.

10) findKey (BTreeHandle *tree, Value *key, RID *result)

	Find a key in B+ tree and return the RID result.

11) insertKey (BTreeHandle *tree, Value *key, RID rid)

	Insert a key into a B+ tree.

12) deleteKey (BTreeHandle *tree, Value *key)

	Delete a key from a B+ tree.

13) openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)

	Initialize the scan handle and prepare for scan.

14) nextEntry (BT_ScanHandle *handle, RID *result)

	Return next entry during scanning.

15) closeTreeScan (BT_ScanHandle *handle)

	Stop scanning and free resources.

16) printTree (BTreeHandle *tree)

	return the printing of the tree in the format. This is a debug function.

17) DFS(RM_BtreeNode *bTreeNode)

	Do a pre-order tree walk and assign positions to tree nodes.

18) walk(RM_BtreeNode *bTreeNode, char *result)

	Do a pre-order tree walk and generate the printing string.

19) createNewNod()

	return a newly created B+ tree node.

20) insertParent(RM_BtreeNode *left, RM_BtreeNode *right, int key)

	Insert key into non-leaf B+ tree node recursively.

21) deleteNode(RM_BtreeNode *bTreeNode, int index)

	Delete the node recursively if needed.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extra Credits:

1) Allow different data types as keys

	This program allows other data types(Value) introduced in assignment 3 to be used as keys for the B+-tree.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
