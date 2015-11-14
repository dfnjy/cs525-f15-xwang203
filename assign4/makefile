CC=gcc
CFLAGS=-I.
DEPS = buffer_mgr.h buffer_mgr_stat.h dberror.h dt.h expr.h record_mgr.h storage_mgr.h tables.h
OBJ = storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o expr.o record_mgr.o rm_serializer.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

all: test_expr test_assign3

test_assign3_1.o: test_assign3_1.c
	$(CC) -c test_assign3_1.c

test_expr.o: test_expr.c
	$(CC) -c test_expr.c

test_expr: $(OBJ) test_expr.o
	gcc -o $@ $^ $(CFLAGS)

test_assign3: $(OBJ) test_assign3_1.o
	gcc -o $@ $^ $(CFLAGS)

clean:
	rm -f *.o test_assign3 test_expr
