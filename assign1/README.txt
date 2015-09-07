

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Assignment-1: 

	The assignment is to accomplish a storage manager of a database. All rights reserved.

* Created by:
	Yinan Zhang 

* Cooperated with:
	Xiao Wang
	Pengyu Guo

* Included files:

	Makefile
	README.txt
	dberror.c
	dberror.h
	storage_mgr.c
	storage_mgr.h
	test_assign1_1.c
	test_helper.h

PS: Add two error messages to dberror.c file. No additional functions.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Installation instruction:

	0. Log in to server and cd to the directory of the code:


	1. Run “make clean” to clean the compiled files, executable files and log files

$ make clean
rm -f *.o
	2. Run “make” command to compile the code:

$ make
gcc -c -o test_assign1_1.o test_assign1_1.c -I.
gcc -c -o storage_mgr.o storage_mgr.c -I.
gcc -c -o dberror.o dberror.c -I.
gcc -o test_assign1 test_assign1_1.o storage_mgr.o dberror.o -I.berror.o storage_mgr.o test_assign1_2.o
$ ls -l
total 144
-rw-r--r--  1 yinanzhang  staff    391  9  6 20:12 README.txt
-rw-r--r--  1 yinanzhang  staff    642  9  1 12:06 dberror.c
-rw-r--r--  1 yinanzhang  staff   1410  9  1 12:06 dberror.h
-rw-r--r--  1 yinanzhang  staff   1392  9  7 11:30 dberror.o
-rw-r--r--  1 yinanzhang  staff    198  9  6 20:12 makefile
-rw-r--r--  1 yinanzhang  staff   4780  9  6 20:12 storage_mgr.c
-rw-r--r--  1 yinanzhang  staff   1755  9  1 12:06 storage_mgr.h
-rw-r--r--  1 yinanzhang  staff   4752  9  7 11:30 storage_mgr.o
-rwxr-xr-x  1 yinanzhang  staff  14488  9  7 11:30 test_assign1
-rw-r--r--  1 yinanzhang  staff   2611  9  6 20:12 test_assign1_1.c
-rw-r--r--  1 yinanzhang  staff   6688  9  7 11:30 test_assign1_1.o
-rw-r--r--  1 yinanzhang  staff   2467  9  1 12:06 test_helper.h

	3. Run test_assign1 :

$ ./test_assign1
$ 

	4. View the result of the test_assign1 file:



	5. If you want to re-run the executable files, start from step 3. If you want to re-compile the files, start from step 1.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Function Description:

	function name		    description
	——————————————————————  ————————————————————————————————————————————————————————————————
	initStorageManager()	Initialize the StorageManager.

	createPageFile()	Create a page file and write content.When createPageFile, we first create an array of char in size
				 of PAGE_SIZE then write this array to file on disk as first page. In the beginning of the file we 
				write an integer which equals to the total pages included in the file, then a following char ‘\n’ 
				which means first page will start.

	openPageFile()		Open the created pageFile and store relative information into fHandle.

	closePageFile()		Close the file.

	destroyPageFile()	Destroy (delete) a page file.

	readBlock()		The method reads the pageNum-th block from a file and
				stores its content in the memory pointed to by the memPage page handle.
				If the file has less than pageNum pages, the method should return RC_READ_NON_EXISTING_PAGE.

	getBlockPos()		Return the current page position in a file.

	readFirstBlock()	Return the first page position of a file.

	readPreviousBlock()	Read page from the previous block.

	readCurrentBlock()	Read page from the Current block.

	readNextBlock()		Read one page from the Next block.

	readLastBlock()		Read page from the last block.

	writeBlock()		Write one page BACK to the file (disk) start from absolute position.

	writeCurrentBlock()	Write one page BACK to the file (disk) of current position.

	appendEmptyBlock()	Write an empty page to the file (disk) by appending to the end.

	ensureCapacity()	If the file has less than numberOfPages pages then increase the size to numberOfPages.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




