CC = gcc
CFLAGS = -Wall -g

all: disk_sort max_ave_followers

dsik_sort: disk_sort.o

max_ave_followers: max_ave_followers.o

%.o: %.c Merge.h
	gcc -Wall -g -c $<

clean: 
	rm -f *.o all *~