CC=gcc
CFLAGS=-I.
DEPS = storage_mgr.h
OBJ = test_assign1_1.o storage_mgr.o dberror.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign1: $(OBJ)
	gcc -o $@ $^ $(CFLAGS)

clean:
	rm -f *.o
