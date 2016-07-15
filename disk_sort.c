/* 
 * CSC443 Assignment 2. External-Memory Sorting
 * Task: implementation of the Two-Pass Multiway Merge Sort (2PMMS), 
 *       and the performance experiments
 *
 * main.c
 * disk_sort.c
 *
 * Created by Seungkyu Kim on Feb 19, 2016
 * Copyright © 2016 Seungky Kim. All rights reserved.
 * 
 **/

/* return: -1 if there is an error. */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/timeb.h>
#include "Merge.h"

/*
 * ================= Phase 1 =================
 */

int compare (const void *a, const void *b) {

   	/*
	* Compares two records a and b 
	* with respect to the value of the integer field uid2.
	* Returns an integer which indicates relative order: 
	* positive: record a > record b
	* negative: record a < record b
	* zero: equal records
	*/


	int a_uid2 = ((const struct Record *)a)->uid2;
	int b_uid2 = ((const struct Record *)b)->uid2;
	return (a_uid2 - b_uid2);
}

/* sorts each partition in turn, writes it to a temp disk file, returns zero on success */
int makeRun (SortingManager manager){

	/*
	* Arguments:
	* 1 - an array to sort
	* 2 - size of an array
	* 3 - size of each array element
	* 4 - function to compare two elements of the array
	
	qsort (buffer, total_records, sizeof(Record), compare);
	*/

	/* sort the given buffer in manager */
	qsort(manager.partitionBuffer, manager.totalRecords, sizeof(Record), compare);

	/* wite the sorted records in buffer to output file */
	fwrite(manager.partitionBuffer, sizeof(Record), manager.totalRecords, manager.inputFile);
	return 0;
} 

/*
 * ================= Phase 2 =================
 */

int refillBuffer(MergeManager *merger, int run_id){

	int bytes_read;
	int remain_records = (merger->inputBuffers+run_id)->sortedPartitionSize / sizeof(Record);

	int flag = (remain_records / (merger->inputBuffers+run_id)->capacity);
	// amount of capacity of records can be still read from input file
	if(flag > 0){
		bytes_read = fread((merger->inputBuffers+run_id)->buffer, sizeof(Record), (merger->inputBuffers+run_id)->capacity, merger->inputFP);
		(merger->inputBuffers+run_id)->currPositionInFile += (bytes_read * sizeof(Record));
		(merger->inputBuffers+run_id)->currentBufferPosition = 0;
		(merger->inputBuffers+run_id)->sortedPartitionSize -= (bytes_read * sizeof(Record));

	// amount of capacity of records is bigger than the remain records from the sorted partitions inputfile
	} else {
		bytes_read = fread((merger->inputBuffers+run_id)->buffer, sizeof(Record), remain_records, merger->inputFP);
		(merger->inputBuffers+run_id)->currPositionInFile += (bytes_read * sizeof(Record));
		(merger->inputBuffers+run_id)->currentBufferPosition = 0;
		(merger->inputBuffers+run_id)->sortedPartitionSize -= (bytes_read * sizeof(Record));
		(merger->inputBuffers+run_id)->totalElements = remain_records;
		(merger->inputBuffers+run_id)->done = 1;

	}
	return 0;
}

/* reads the next element from an input buffer */
int getNextRecord (MergeManager *merger, int run_id, Record *result) {

	// read all record in buffer so need to read record from inputfile
	if((merger->inputBuffers+run_id)->totalElements == (merger->inputBuffers+run_id)->currentBufferPosition){
		// there are still records can be read from the sorted partition input file
		if ((merger->inputBuffers+run_id)->done != 1){

			int outcome = refillBuffer(merger, run_id);
			if(outcome != 0){
				printf("RefillBuffer failed\n");
				return(1);
			} 
			if ((merger->inputBuffers+run_id)->done != 1) {
				// assign next record from the input buffer and update attributes of inputBuffer
				*result = (merger->inputBuffers+run_id)->buffer[(merger->inputBuffers+run_id)->currentBufferPosition];
				(merger-> inputBuffers+run_id)->currentBufferPosition += 1;
			}
		} else {
			return -1;
		}
	} else {
		*result = (merger->inputBuffers+run_id)->buffer[(merger->inputBuffers+run_id)->currentBufferPosition];
		(merger-> inputBuffers+run_id)->currentBufferPosition += 1;
	}

	return 0;
}

int flushOutputBuffer(MergeManager *merger){


	if(merger->currentPositionInOutputBuffer == merger->outputBufferCapacity ) {
		fwrite(merger->outputBuffer, sizeof(Record), merger->outputBufferCapacity, merger->outputFP);
	} else {
		fwrite(merger->outputBuffer, sizeof(Record), merger->currentPositionInOutputBuffer, merger->outputFP);
	}

	return 0;
}


int getTopHeapElement (MergeManager *merger, Record *result) {
	Record item;
	int child, parent;

	if(merger->heapSize == 0) {
		printf( "UNEXPECTED ERROR: popping top element from an empty heap\n");
		return 1;
	}

	*result=merger->heap[0];  //to be returned

	//now we need to reorganize heap - keep the smallest on top
	item = merger->heap [-- merger->heapSize]; // to be reinserted 

	parent =0;
	while ((child = (2 * parent) + 1) < merger->heapSize) {
		// if there are two children, compare them 
		if (child + 1 < merger->heapSize && (compare((void *)&(merger->heap[child]),(void *)&(merger->heap[child + 1]))>0)) {
			++child;
		}
		// compare item with the larger 
		if (compare((void *)&item, (void *)&(merger->heap[child])) > 0) {
			merger->heap[parent] = merger->heap[child];
			parent = child;
		} else {
			break;
		}
	}
	merger->heap[parent] = item;

	// finding run_id of the top element
	int num_buffers = merger->heapCapacity;
	int i;
	for(i=0; i<num_buffers; i++){
		if((result->uid1 == (merger->heapRecord+i)->uid1) && (result->uid2 == (merger->heapRecord+i)->uid2)){
			//printf("find!\n");
			merger->runID = (merger->heapRecord+i)->run_id;
			break;
		} 
	}

	

	return 0;
}

int insertIntoHeap (MergeManager *merger, int run_id, Record *record) {
	
	(merger->heapRecord+run_id)->uid1 = record->uid1;
	(merger->heapRecord+run_id)->uid2 = record->uid2;
	(merger->heapRecord+run_id)->run_id = run_id;

	int child, parent;
	if (merger->heapSize == merger->heapCapacity)	{
		printf( "Unexpected ERROR: heap is full\n");
		return 1;
	}	
  	
	child = merger->heapSize++; /* the next available slot in the heap */
	
	while (child > 0)	{
		parent = (child - 1) / 2;
		if (compare((void *)&(merger->heap[parent]),(void *)record)>0) {
			merger->heap[child] = merger->heap[parent];
			child = parent;
		} else 	{
			break;
		}
	}
	merger->heap[child]= *record;	
	return 0;
}

/* initial fill of input buffers with elements of each run */
int initInputBuffers(MergeManager *merger){ 

 	int num_runs = merger->heapCapacity;
 	int i;
 	int bytes_read = 0;
 	int num_records_per_buffer =  merger->outputBufferCapacity; // number of records in one buffer

	for (i=0; i< num_runs;i++){
		bytes_read = fread((merger->inputBuffers+i)->buffer, sizeof(Record), num_records_per_buffer, merger->inputFP);
		(merger->inputBuffers+i)->currPositionInFile = (merger->inputBuffers+i)-> currPositionInFile + (bytes_read );
		(merger->inputBuffers+i)->sortedPartitionSize -= (bytes_read * sizeof(Record));
	}

	return 0;
}


/* inserts into heap one element from each buffer - to keep the smallest on top */
int initHeap(MergeManager *merger) {

	int i;
	int result;
	for (i=0; i < merger->heapCapacity; i++){
		Record* record = (Record *)(merger->inputBuffers+i)->buffer;
		result = insertIntoHeap(merger, i, record);
		if (result != 0){
			printf("insertIntoHeap Error:\n");
			return(1);
		}
		(merger->inputBuffers+i)->currentBufferPosition += 1;
	}
	return 0;
}

int mergeRuns (MergeManager *merger) { //merger should be already set up at this point

	int  result;
	//1. go in the loop through all input files and fill-in initial buffers
	if (initInputBuffers(merger)!= 0)
		return 1;
	
	//2. Initialize heap with 1 element from each buffer
	if (initHeap(merger)!= 0)
		return 1;	

	while (merger->heapSize > 0) { //heap is not empty	
		Record smallest;
		Record next;

		if (getTopHeapElement (merger, &smallest)!=0)
			return 1;

		int runID = merger->runID;	
	 	merger->outputBuffer[merger->currentPositionInOutputBuffer++]=smallest;
		result = getNextRecord (merger, runID, &next);

		if(result != -1) {// next element exists	
			if(insertIntoHeap (merger, runID, &next)!=0)
				return 1;
		} 
		if(result==1) //error
			return 1;
		
		if(merger->currentPositionInOutputBuffer == merger-> outputBufferCapacity ) { //staying on the last slot of the output buffer - next will cause overflow
			if(flushOutputBuffer(merger)!=0)
				return 1;			
			merger->currentPositionInOutputBuffer=0;
		}
	}
	// flush what remains in output buffer
	if(merger->currentPositionInOutputBuffer >0) { //there are elements in the output buffer
		if(flushOutputBuffer(merger)!=0)
			return 1;
	}

	return 0;	
}

/* $disk_sort <name of the input file> <total mem in bytes> <block size> <number of runs>(K)*/
int main(int argc, char *argv[]) {

	FILE *fp_read, *fp_write; 
	char *inFile = argv[1];
	int total_mem = atoi(argv[2]);
	int block_size = atoi(argv[3]);
	int num_runs = atoi(argv[4]);

	int num_blocks = 0;
	int num_records = 0;

	int total_records = 0;
	
	/* check correct number of arguments */
	if (argc != 5) {
		printf("Usage: Number of Arguments are not satisfied \n");
		return(-1);
	}
	/* open binary file for reading */
	if (!(fp_read = fopen(inFile, "rb+"))) {
		printf("Invalid input file: %s\n", inFile);
		return(-1);
	}
	/* open output file */
	if(!(fp_write = fopen("Output.dat", "wb+"))){
		printf("Invalid output file: Output.txt\n");
		return(-1);
	}

	/* maximum available memory should be below 200MB */
	if (total_mem > 200000000){
		printf("Inavlid input size of available memory: %d\n", total_mem);
		return(-1);
	}

	/* find the size of input file in bytes */
	fseek(fp_read, 0L, SEEK_END);
	int input_size = ftell(fp_read);
	fseek(fp_read, 0L, SEEK_SET); // seek back to the beginning

	/* assign size of each partition */
	int partition_size = input_size / num_runs;
	if (partition_size > total_mem){
		printf("size of each partition is exceeded to total memory\n");
		return(-1);
	}

/*
 * ================= Phase 1 =================
 */

	total_records = input_size / sizeof(Record);
	num_blocks = partition_size / block_size;
	
	int records_per_partition = (num_blocks * block_size) / sizeof(Record);
	int sorted_partition_size  = num_blocks * block_size;

	/* allocate buffer for a partition */
	Record *buffer_partition = (Record *)malloc(num_blocks * block_size);
	
	int i;
	int offset = 0;
	size_t bytes_read;
	size_t total_bytes_read = 0L;

	/* compute all partitions except the last one */
	for (i=1; i<=num_runs; i++){

		/* read records thaat fits the size of buffer */
		if (i != (num_runs)){

			/* reading input file with partition size */
			SortingManager sort_manager;
			
			bytes_read = fread(buffer_partition, sizeof(Record), records_per_partition, fp_read);
			sort_manager.partitionBuffer = buffer_partition;
			sort_manager.inputFile = fp_read;
			sort_manager.totalRecords = records_per_partition;
			sort_manager.totalPartitions = num_runs;
			num_records += records_per_partition;

			fseek(fp_read, offset, SEEK_SET);

			/* sort records in buffer and write to output file */
			int sorted_partition = makeRun(sort_manager);
			if(sorted_partition){
				perror("Error MakeRun:\n");
				return(-1);
			}		
			break;

			total_bytes_read += bytes_read;
			offset += (bytes_read * sizeof(Record));
			
		/* read the last chunk of remaining records that may not fit buffer alloced with partition size */
		} else {
			int remain_records = 0;
			/* calculate size of remain records in input file */
			int remain = input_size - offset;
			Record *buffer_remain = (Record *) malloc (remain);
			remain_records = total_records - num_records;

			bytes_read = fread(buffer_remain, sizeof(Record), remain_records, fp_read);
	
			fseek(fp_read, offset, SEEK_SET);

			SortingManager sort_manager;

			sort_manager.partitionBuffer = buffer_remain;
			sort_manager.inputFile = fp_read;
			sort_manager.totalRecords = remain_records;
			sort_manager.totalPartitions = num_runs;

			/* sort records in buffer and write to output file */
			int sorted_partition = makeRun(sort_manager);
			if(sorted_partition){
				perror("Error MakeRun:\n");
				return(-1);
			}

			/* free allocated memory */
			free(buffer_remain);
		}
	}

	/* free allocated memory */
	free(buffer_partition);

/*
 * ================= Phase 2 =================
 */

	/* set reading and writing point to biginning*/
	fseek(fp_read, 0L, SEEK_SET);

	/* calculate size of input and output buffer */
	int single_buffer_size = (total_mem - sizeof(MergeManager) - sizeof(HeapRecord) - (num_runs * sizeof(Record))) / (num_runs + 1); // +1 is for output buffer, should not count size of MergeManager header
	int single_buffer_size_without_header = single_buffer_size - sizeof(InputBuffer);
	num_blocks = single_buffer_size_without_header / block_size;
	int records_in_single_buffer_without_head = num_blocks * block_size / sizeof(Record);

	// allocate memory for MergeManager header 
	MergeManager *merge_manager = (MergeManager *) malloc( sizeof(MergeManager));
	// allocate memory for OutputBuffer
	Record *buffer_output = (Record *) malloc (single_buffer_size_without_header);
	// allocate memory for inputBuffer header
	InputBuffer *input_buffer = (InputBuffer *) malloc( sizeof(InputBuffer) * num_runs );
	// allocates memory for all input buffers
	Record *buffer_mem = (Record *) malloc (num_blocks * block_size * num_runs);
	// allocates memory for heap
	Record *heap_mem = (Record * ) malloc (num_runs * sizeof(Record));
	// allocates memory for heapRecord
	HeapRecord *heap_record = (HeapRecord *) malloc (num_runs * sizeof(HeapRecord));

	int j;
	for(j=0; j<num_runs; j++){
		(input_buffer+j)->capacity = records_in_single_buffer_without_head;
		(input_buffer+j)->runLength = j;
		(input_buffer+j)->currPositionInFile = sorted_partition_size * j;
		(input_buffer+j)->currentBufferPosition = 0;			
		(input_buffer+j)->totalElements = num_blocks * block_size / sizeof(Record);
		(input_buffer+j)->done = 0;
		(input_buffer+j)->sortedPartitionSize = sorted_partition_size;
		(input_buffer+j)->buffer = buffer_mem + (records_in_single_buffer_without_head * j);
	}

	/* assign data to MergeManager */
	merge_manager->heap = heap_mem;
	merge_manager->heapSize = 0;
	merge_manager->heapCapacity = num_runs;
	merge_manager->inputFP = fp_read;
	merge_manager->outputFP = fp_write;
	merge_manager->outputBuffer = buffer_output;
	merge_manager->currentPositionInOutputBuffer = 0;
	merge_manager->outputBufferCapacity = records_in_single_buffer_without_head;
	merge_manager->inputBuffers = input_buffer;
	merge_manager->heapRecord = heap_record;

	int result_merge = mergeRuns(merge_manager);
	if(result_merge){
		printf("Failed mergeRuns!!\n");
		return(-1);
	}

	free(merge_manager);
	free(buffer_output);
	free(buffer_mem);
	free(heap_mem);

	/* close open files for reading and writting */
	fclose(fp_read);
	fclose(fp_write);
	
	return(0);
}
