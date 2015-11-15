CC=gcc
CFLAGS=-I.
DEPS = contest.h test_helper.h btree_mgr.h buffer_mgr.h buffer_mgr_stat.h dberror.h dt.h expr.h record_mgr.h storage_mgr.h tables.h
OBJ = contest.o contest_setup.o btree_mgr.o storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o expr.o record_mgr.o rm_serializer.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

contest: $(OBJ)
	gcc -o $@ $^ $(CFLAGS)

clean:
	rm -f *.o contest
