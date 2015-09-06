CC=gcc
CFLAGS=-I.
DEPS = buffer_mgr.h storage_mgr.h
OBJ = test_assign2_1.o storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign2: $(OBJ)
	gcc -o $@ $^ $(CFLAGS)

clean:
	rm -f *.o
