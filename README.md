# External_Memory_Sorting

External sorting is a class of sorting algorithms that can handle massive amounts of data. External sorting is required when the data being sorted do not fit into the main memory of a computing device (usually RAM) and instead they must reside in the slower external memory, usually a hard disk drive. Thus, external sorting algorithms are external memory algorithms and thus applicable in the external memory model of computation.

External sorting algorithms generally fall into two types, distribution sorting, which resembles quicksort, and external merge sort, which resembles merge sort. The latter typically uses a hybrid sort-merge strategy. In the sorting phase, chunks of data small enough to fit in main memory are read, sorted, and written out to a temporary file. In the merge phase, the sorted subfiles are combined into a single larger file.

## 1. Implementing 2PMMS
The suggested sequence of tasks for implementing 2PMMS is outlined below. Let's call our program disk_sort. Test each step after implementing it. Keep in mind that if you understand the 2PMMS algorithm, you do not have to use these suggestions

### 1.1. Main-memory sorting
Implement a function that sorts an array of records by UID2 - in RAM. Use the stable in-place quick sort implemented in C function qsort  (<stdlib.h>). The qsort function requires that you provide the way of comparing two array entries, as in the following example:
```
 /**
 * Compares two records a and b 
 * with respect to the value of the integer field f.
 * Returns an integer which indicates relative order: 
 * positive: record a > record b
 * negative: record a < record b
 * zero: equal records
 */
 int compare (const void *a, const void *b) {
  int a_f = ((const struct Record*)a)->f;
  int b_f = ((const struct Record*)b)->f;
  return (a_f - b_f);
 }

 This function can now be used as an argument to qsort. Sample call to qsort is shown below:

 /**
 * Arguments:
 * 1 - an array to sort
 * 2 - size of an array
 * 3 - size of each array element
 * 4 - function to compare two elements of the array
 */
 qsort (buffer, total_records, sizeof(Record), compare);
```
 Read a part of your input file into a buffer array of a size which is a multiple of your block size, sort records by UID2, and write the sorted buffer to stdout. Test that the output records are indeed sorted by UID2.

### 1.2. Producing sorted runs
Use the sorting routine developed in 1.1. to implement Phase I of 2PMMS. The program should accept the following parameters: <name of the input file>, <total mem in bytes>, and <block size>. The total memory parameter is roughly the amount of memory you are allowed to use in your program. The program will also check if the size of the allocated memory is sufficient to perform 2PMMS, and will exit gracefully if there is not enough memory for a two-pass algorithm.

You may use additional 5MB of memory for bookkeeping data structures: for example if <total mem in bytes> is set to 200MB, then you may use no more than 205 MB in your program.

Partition binary input into K chunks of maximum possible size, such that each chunk can be sorted with the available total memory. The program will determine the size of each chunk and align it with the block size. After that, it will sort records in each chunk by UID2 and write each sorted run to disk.

Do not forget to free all dynamically allocated arrays after the completion of this step.

### 1.3. Merging runs
In this part, you will need to allocate memory for K input buffers and for one output buffer. Each input buffer should contain an array of records, which is allocated dynamically, and its size depends on the amount of the available main memory and the total number of runs K. Make sure that the size of each such array is aligned with the block size. Do not forget to account for the size of the output buffer, which should be able to hold at least one block.

In addition to an array of records, for each input buffer we need to store: its corresponding file pointer, the current position in this file, and the current position in the buffer itself.

The merge starts with pre-filling of input buffer arrays with records from each run. The suggestion is to add the heads of each array to a heap data structure, and then remove an element from the top of the heap, transfer it to the output buffer, and insert into the heap the next element from the same run as the element being transferred.

When any input buffer array is processed, it gets refilled from the corresponding run, until the entire run has been processed. When the output buffer is full, its content is appended to the final output file.
The program terminates when the heap is empty - all records have been merged. Do not forget to flush to disk the remaining content of the output buffer.

The starter code is provided in this repository. Again, you are free to ignore this starter code and follow your own program design.

## 2. Performance experiments
### 2.1. Timing
Run your disk_sort program with 200MB of available memory, and make sure that you never allocate more than 200MB of memory buffers. Record performance and memory consumption. You may use the following timing command:
```
$ /usr/bin/time -v disk_sort <input file> <mem> <block size> 
```
Record the total elapsed time taken by your program, and the value of the maximum resident set size.
Use this timing utility in all the experiments. Always record the total time and the pick memory consumption.

### 2.2. Buffer size
Experiment with different amounts of available main memory. Try memory sizes of 1/2, 1/4, 1/16 ... of the original 200MB, until the program cannot perform the two-pass algorithm anymore. Perform each experiment on a freshly-booted machine (or use the concatenated input file which is at least 1.5 times larger than the total main memory on this machine, or create a new copy of the input file before each experiment), in order to avoid system caching of the entire file.

In each experiment we perform at most 4 disk I/Os per input block, and theoretically the running time should not depend on the number of runs K, and the sizes of input and output buffers.


### 2.3. Comparing to Unix sort
Let's compare the running time of our implementation (with 200 MB of RAM) to the Unix sort. The Unix sort also uses the external-memory merge sort for sorting large files. The input to the Unix program is our original text file edges.csv. Time the following program:
```
$ sort -t"," -n -k2 edges.csv > edges_sorted_uid2.csv 
```

## 3. Research: Twitter graph degree distributions
Having available two files of records - one sorted by UID1 and another sorted by UID2 - it is time to modify your max_average program from Assignment 1.1 to find out the counts of nodes with each value of out- and in- degrees. 

Because you know the maximum number of out-degrees (and similarly you can find the maximum number of in-degrees), for simplicity, you may use an array of this size to keep counts. However, if you want to make your program more general, you may use a hashtable, if you know how to implement it in C. If you decide to use an array, you need to add an additional parameter to your program: max_degree.

Write a new program called distribution which accepts as a parameter the name of the corresponding sorted file, the block size and the column id, and produces the required histogram. The column ID parameter tells which UID should we use for the computation - either UID1 (for out-degree) or UID2 (for in-degree).
```
$ distribution <file_name> <block_size> <column_id> [<max_degree>]
```
 After you compute both histograms, you now test if any of them exhibits a power-law distribution. There's a simple method that provides a quick test for this.

Let f(k) be the fraction of items that have value k, and suppose you want to know whether the equation f(k) = a/kc approximately holds, for some exponent c and constant of proportionality a. Then, if we write this as f(k) = a/kc and take the logarithms of both sides of this equation, we get:
```
log f (k) = log a - c log k
```
This says that if we have a power-law relationship, and we plot log f(k) as a function of log k, then we should see a straight line: -c will be the slope, and log a will be the y-intercept. Such a "log-log" plot thus provides a quick way to see if one's data exhibits an approximate power-law distribution: it is easy to see if one has an approximately straight line, and one can read off the exponent from the slope. The Figure 2 shows an example for web page incoming links.

