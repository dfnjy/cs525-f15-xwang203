=== Coding Ideas ===
Our goal is to implement a storage manager. The code should provide interfaces dealing with pages and blocks, including creating, reading, writing, deleting, etc.
We implemented each function defined in the header file according to the function descriptions on the assignment page. Tests and memory checks were made subsequently.
=== Code Implementation Description ===
When createPageFile, we first create an array of char in size of PAGE_SIZE then write this array to file on disk as first page. In the beginning of the file we write an integer which equals to the total pages included in the file, then a following char ‘\n’ which means first page will start.
When openPageFile, we open the file on disk, then parse the parameters and write them into memory(fHandle).
When closePageFile, we set the pointers to fHandle to NULL after closing the file.
When destroyPageFile, we remove the file on disk directly.
