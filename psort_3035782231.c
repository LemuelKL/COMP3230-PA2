// COMP3230 Programming Assignment Two
// The parallel version of samplesort

/*
# Filename: psort_3035782231.c
# Student name and No.:	Lee Kwok Lam, 3035782231
# Development platform:	WSL 2 Ubuntu 20.04 on Windows 11
# Remark:
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <assert.h>
#include <pthread.h>

int checking(unsigned int *, long);
int compare(const void *, const void *);
int ssort(unsigned int *, long);
void *phase1(void *);
void *phase3(void *);
void *phase4(void *);

// global variables
long size;			  // size of the array
unsigned int *intarr; // array of random integers
int p;				  // number of worker threads
int *pivot_values;	  // array of pivot values
int *partition_sizes; // array of partition sizes,
					  // used as if it were a 2D array
					  // partition_sizes[i][j] is the
					  // size of the jth partition of the ith thread
int *bucket_sizes;	  // array of bucket sizes

// use the example array in the assignment
int example()
{
	size = 40;
	p = 4;
	intarr[0] = 42;
	intarr[1] = 98;
	intarr[2] = 2;
	intarr[3] = 31;
	intarr[4] = 86;
	intarr[5] = 87;
	intarr[6] = 5;
	intarr[7] = 13;
	intarr[8] = 99;
	intarr[9] = 44;
	intarr[10] = 67;
	intarr[11] = 37;
	intarr[12] = 17;
	intarr[13] = 7;
	intarr[14] = 87;
	intarr[15] = 3;
	intarr[16] = 96;
	intarr[17] = 71;
	intarr[18] = 40;
	intarr[19] = 19;
	intarr[20] = 58;
	intarr[21] = 13;
	intarr[22] = 61;
	intarr[23] = 77;
	intarr[24] = 11;
	intarr[25] = 13;
	intarr[26] = 6;
	intarr[27] = 81;
	intarr[28] = 76;
	intarr[29] = 18;
	intarr[30] = 24;
	intarr[31] = 14;
	intarr[32] = 63;
	intarr[33] = 59;
	intarr[34] = 99;
	intarr[35] = 17;
	intarr[36] = 36;
	intarr[37] = 84;
	intarr[38] = 1;
	intarr[39] = 48;
}

int main(int argc, char **argv)
{
	long i, j;
	struct timeval start, end;

	if ((argc < 2))
	{
		printf("Usage: psort <number> [<no_of_workers>]\n");
		exit(0);
	}

	p = 4; // default to 4 worker threads
	if (argc == 3)
	{
		p = atoi(argv[2]);
		if (!(p > 1))
		{
			printf("<no_of_workers> must be greater than 1\n");
			exit(0);
		}
	}

	size = atol(argv[1]);
	intarr = (unsigned int *)malloc(size * sizeof(unsigned int));
	if (intarr == NULL)
	{
		perror("malloc");
		exit(0);
	}

	// set the random seed for generating a fixed random
	// sequence across different runs
	char *env = getenv("RANNUM"); // get the env variable
	if (!env)					  // if not exists
		srandom(3230);
	else
		srandom(atol(env));

	for (i = 0; i < size; i++)
		intarr[i] = random();

	// measure the start time
	gettimeofday(&start, NULL);

	pthread_t *workers = (pthread_t *)malloc(p * sizeof(pthread_t));
	for (int i = 0; i < p; i++)
	{
		int *pointer = (int *)malloc(sizeof(int));
		*pointer = i;
		pthread_create(workers + i, NULL, &phase1, (void *)pointer);
	}

	// gather samples from workers
	int *all_samples = (int *)malloc(p * p * sizeof(int));
	for (int i = 0; i < p; i++)
	{
		int *return_samples = (int *)malloc(p * sizeof(int));
		pthread_join(workers[i], (void **)&return_samples);
		for (int j = 0; j < p; j++)
			all_samples[i * p + j] = return_samples[j];
		free(return_samples);
	}

	qsort(all_samples, p * p, sizeof(unsigned int), compare);

	// get the pivot values
	pivot_values = (int *)malloc((p - 1) * sizeof(int));
	for (int i = 0; i < p - 1; i++)
		pivot_values[i] = all_samples[(i + 1) * p + (p / 2) - 1];

	// initialize the partition sizes array
	partition_sizes = (int *)malloc(p * p * sizeof(int));
	for (int i = 0; i < p * p; i++)
		partition_sizes[i] = 0; // default to zero

	// workers compute their partition sizes
	for (int i = 0; i < p; i++)
	{
		int *pointer = (int *)malloc(sizeof(int));
		*pointer = i;
		pthread_create(workers + i, NULL, &phase3, (void *)pointer);
	}
	for (int i = 0; i < p; i++)
		pthread_join(workers[i], NULL);

	// print out the partition sizes
	// printf("Partition sizes:\n");
	// for (int i = 0; i < p; i++)
	// {
	// 	printf("TH[%d] ", i);
	// 	for (int j = 0; j < p; j++)
	// 		printf("%d ", partition_sizes[i * p + j]);
	// 	printf("\n");
	// }
	// printf("\n");

	// workers gather their own buckets
	// each worker has p buckets
	// each bucket is returned as an pointer to an array
	bucket_sizes = (int *)malloc(p * sizeof(int));
	for (int i = 0; i < p; i++)
	{
		int *pointer = (int *)malloc(sizeof(int));
		*pointer = i;
		pthread_create(workers + i, NULL, &phase4, (void *)pointer);
	}
	int **return_bucket_pointers = (int **)malloc(p * sizeof(int *));
	for (int i = 0; i < p; i++)
		pthread_join(workers[i], (void **)&return_bucket_pointers[i]);

	// sum up the bucket sizes and verify
	int sum_bucket_size = 0;
	for (int i = 0; i < p; i++)
		sum_bucket_size += bucket_sizes[i];
	assert(sum_bucket_size == size);

	// merge the buckets and insert into the final array
	int res_idx = 0;
	for (int i = 0; i < p; i++)
		for (int j = 0; j < bucket_sizes[i]; j++)
			intarr[res_idx++] = return_bucket_pointers[i][j];

	// end slavery
	free(bucket_sizes);
	for (int i = 0; i < p; i++)
		free(return_bucket_pointers[i]);
	free(return_bucket_pointers);
	free(partition_sizes);
	free(pivot_values);
	free(all_samples);
	free(workers);

	// measure the end time
	gettimeofday(&end, NULL);

	if (!checking(intarr, size))
		printf("The array is not in sorted order!!\n");

	printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec) * 1.0 + (end.tv_usec - start.tv_usec) / 1000000.0);

	free(intarr);
	return 0;
}

// each worker thread runs this function
// each worker sorts its own subsequence
// then it samples p elements from the sorted subsequence
// and returns the samples to the main thread
void *phase1(void *argc)
{
	int i = *(int *)argc;
	int start = i * size / p;
	int end = (i + 1) * size / p;
	if (i == p - 1)
		end = size;

	qsort(intarr + start, end - start, sizeof(unsigned int), compare);

	int *result_samples = (int *)malloc(p * sizeof(int));
	for (int j = 0; j < p; j++)
		result_samples[j] = intarr[start + j * size / (p * p)];
	return (void *)result_samples;
}

// each worker thread runs this function
// computes its own partition sizes
void *phase3(void *argc)
{
	int i = *(int *)argc;
	int start = i * size / p;
	int end = (i + 1) * size / p;
	if (i == p - 1)
		end = size;

	int pivot_idx = 0;
	int prt_size = 0; // the size of the current partition
	for (int j = start; j < end; j++)
	{
		if (intarr[j] <= pivot_values[pivot_idx])
		{
			prt_size++;
			continue;
		}
		// keep assigning prt_size to the current partition
		// until the next pivot value is reached
		while (pivot_idx < p - 1 && intarr[j] > pivot_values[pivot_idx])
		{
			partition_sizes[i * p + pivot_idx] = prt_size;
			prt_size = 0;
			pivot_idx++;
		}
		prt_size++;
	}
	// take care of the last partition
	if (prt_size > 0)
		partition_sizes[i * p + pivot_idx] = prt_size;

	// printf("(Phase3) TH[%d] ", i);
	// for (int j = start; j < end; j++)
	// 	printf("%d ", intarr[j]);
	// printf("\n");
}

void *phase4(void *argc)
{
	int i = *(int *)argc;
	int *bucket = (int *)malloc(size * sizeof(int));
	int bucket_idx = 0;
	for (int target_th = 0; target_th < p; target_th++)
	{
		int start = target_th * size / p;
		int partition_size;
		for (int j = 0; j < i; j++)
		{
			partition_size = partition_sizes[target_th * p + j];
			start += partition_size;
		}
		partition_size = partition_sizes[target_th * p + i];

		// useful for debug
		// printf("TH[%d] [%d] \t+ %d\n", i, start, partition_size);

		for (int j = 0; j < partition_size; j++)
			bucket[bucket_idx++] = intarr[start + j];
	}
	qsort(bucket, bucket_idx, sizeof(unsigned int), compare);

	// print the sorted bucket
	// printf("(Phase 4) TH[%d] ", i);
	// for (int j = 0; j < bucket_idx; j++)
	// 	printf("%d ", bucket[j]);
	// printf("\n");

	bucket_sizes[i] = bucket_idx;

	return (void *)bucket;
}

int compare(const void *a, const void *b)
{
	return (*(unsigned int *)a > *(unsigned int *)b) ? 1 : ((*(unsigned int *)a == *(unsigned int *)b) ? 0 : -1);
}

int checking(unsigned int *list, long size)
{
	long i;
	// printf("First : %d\n", list[0]);
	// printf("At 25%%: %d\n", list[size / 4]);
	// printf("At 50%%: %d\n", list[size / 2]);
	// printf("At 75%%: %d\n", list[3 * size / 4]);
	// printf("Last  : %d\n", list[size - 1]);
	for (i = 0; i < size - 1; i++)
		if (list[i] > list[i + 1])
			return 0;
	return 1;
}
