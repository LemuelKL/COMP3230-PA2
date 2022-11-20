// COMP3230 Programming Assignment Two
// The sequential version of the sorting using qsort

/*
# Filename:
# Student name and No.:
# Development platform:
# Remark:
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
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
int no_of_workers;
int *pivot_values;
int *pivot_locs;
int *bucket_sizes;

int example_populate()
{
	size = 40;
	intarr = (unsigned int *)malloc(size * sizeof(unsigned int));
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
	return 0;
}

int main(int argc, char **argv)
{
	long i, j;
	struct timeval start, end;

	if ((argc < 2))
	{
		printf("Usage: seq_sort <number> [<no_of_workers>]\n");
		exit(0);
	}

	no_of_workers = 4;
	if (argc == 3)
	{
		no_of_workers = atoi(argv[2]);
		if (!(no_of_workers > 1))
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

	// example_populate();

	// measure the start time
	gettimeofday(&start, NULL);

	// printf("Sorting %ld integers using %d workers\n", size, no_of_workers);
	// for (int i = 0; i < size; i++)
	// 	printf("[%d] %d\n", i, intarr[i]);

	// 1. create threads
	// pthread_t master;
	pthread_t *workers = (pthread_t *)malloc(no_of_workers * sizeof(pthread_t));
	// pthread_create(&master, NULL, ssort, (void *)intarr);
	for (int i = 0; i < no_of_workers; i++)
	{
		int *pointer = (int *)malloc(sizeof(int));
		*pointer = i;
		pthread_create(workers + i, NULL,
					   &phase1,
					   (void *)pointer);
		// printf("the %dth thread is created\n", i);
	}
	// 2. all slaves work on phase1 while the master waits
	int *all_samples = (int *)malloc(no_of_workers * no_of_workers * sizeof(int));
	for (int i = 0; i < no_of_workers; i++)
	{
		int *return_samples = (int *)malloc(no_of_workers * sizeof(int));
		pthread_join(workers[i], (void **)&return_samples);
		// printf("the %d-th thread joined\n", i);
		for (int j = 0; j < no_of_workers; j++)
		{
			all_samples[i * no_of_workers + j] = return_samples[j];
		}
		free(return_samples);
	}

	// print the samples
	printf("Samples:\n");
	for (int i = 0; i < no_of_workers * no_of_workers; i++)
	{
		printf("[%d]\t%d\n", i, all_samples[i]);
	}
	printf("\n");

	// 3. master works on phase2 while the slaves wait
	qsort(all_samples, no_of_workers * no_of_workers, sizeof(unsigned int), compare);

	// print sorted samples
	printf("Sorted samples:\n");
	for (int i = 0; i < no_of_workers * no_of_workers; i++)
	{
		printf("[%d]\t%d\n", i, all_samples[i]);
	}
	printf("\n");

	printf("[Pivot locations @ sorted samples]\n");
	pivot_values = (int *)malloc((no_of_workers - 1) * sizeof(int));
	for (int i = 0; i < no_of_workers - 1; i++)
	{
		int loc = (i + 1) * no_of_workers + (no_of_workers / 2) - 1;
		printf("%d ", loc);
		pivot_values[i] = all_samples[loc];
	}
	printf("\n");
	// print the pivot values
	printf("[Pivot values]\n");
	for (int i = 0; i < no_of_workers - 1; i++)
		printf("[%d]\t%d\n", i, pivot_values[i]);

	// 4. all slaves work on phase3 and phase4

	pivot_locs = (int *)malloc(no_of_workers * (no_of_workers - 1) * sizeof(int));
	for (int i = 0; i < no_of_workers; i++)
	{
		int *pointer = (int *)malloc(sizeof(int));
		*pointer = i;
		pthread_create(workers + i, NULL, &phase3, (void *)pointer);
	}
	for (int i = 0; i < no_of_workers; i++)
		pthread_join(workers[i], NULL);

	// print the pivot locations
	printf("[Pivot locations]\n");
	for (int i = 0; i < no_of_workers * (no_of_workers - 1); i++)
	{
		printf("%d\t", pivot_locs[i]);
		if ((i + 1) % (no_of_workers - 1) == 0)
		{
			printf("\n");
		}
	}
	printf("\n");

	// 5. all slaves sort their buckets and die
	// 6. master waits for slaves to die and then merges their buckets, sort them, and dies
	bucket_sizes = (int *)malloc(no_of_workers * sizeof(int));
	for (int i = 0; i < no_of_workers; i++)
	{
		int *pointer = (int *)malloc(sizeof(int));
		*pointer = i;
		pthread_create(workers + i, NULL, &phase4, (void *)pointer);
	}
	int **return_bucket_pointers = (int **)malloc(no_of_workers * sizeof(int *));
	for (int i = 0; i < no_of_workers; i++)
		pthread_join(workers[i], (void **)&return_bucket_pointers[i]);

	for (int i = 0; i < no_of_workers; i++)
		printf("BK[%d] size = %d\n", i, bucket_sizes[i]);

	int res_idx = 0;
	// merge the buckets
	// printf("Buckets:\n");
	for (int i = 0; i < no_of_workers; i++)
	{
		for (int j = 0; j < bucket_sizes[i]; j++)
		{
			// printf("%d ", return_bucket_pointers[i][j]);
			intarr[res_idx++] = return_bucket_pointers[i][j];
		}
		// printf("\n");
	}

	// measure the end time
	gettimeofday(&end, NULL);

	if (!checking(intarr, size))
		printf("The array is not in sorted order!!\n");

	printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec) * 1.0 + (end.tv_usec - start.tv_usec) / 1000000.0);

	free(intarr);
	return 0;
}

void *phase1(void *argc)
{
	int i = *(int *)argc;
	int start = i * size / no_of_workers;
	int end = (i + 1) * size / no_of_workers;
	if (i == no_of_workers - 1)
		end = size;
	printf("the %d-th thread is working on %d to %d\n", i, start, end);
	qsort(intarr + start, end - start, sizeof(unsigned int), compare);

	int *result_samples = (int *)malloc(no_of_workers * sizeof(int));
	for (int j = 0; j < no_of_workers; j++)
	{
		int loc = j * size / (no_of_workers * no_of_workers);
		result_samples[j] = intarr[start + loc];
		printf("worker[%d] sample[%d] @ %d = %d\n", i, j, start + loc, result_samples[j]);
	}
	return (void *)result_samples;
}

void *phase3(void *argc)
{
	int i = *(int *)argc;
	int start = i * size / no_of_workers;
	int end = (i + 1) * size / no_of_workers;
	if (i == no_of_workers - 1)
		end = size;

	int pivot_idx = 0;
	for (int j = start; j < end && pivot_idx < no_of_workers - 1; j++)
	{
		// printf("worker[%d] checking %d\n", i, intarr[j]);
		while (intarr[j] > pivot_values[pivot_idx])
		{
			pivot_locs[i * (no_of_workers - 1) + pivot_idx] = j;
			pivot_idx++;
			if (pivot_idx == no_of_workers - 1)
				break;
		}
	}
}

void *phase4(void *argc)
{
	int i = *(int *)argc;
	int *bucket = (int *)malloc(size * sizeof(int));
	int bucket_idx = 0;
	for (int target_th = 0; target_th < no_of_workers; target_th++)
	{
		int start = -1;
		int end = -1;
		if (i == 0)
		{
			start = target_th * size / no_of_workers;
			end = pivot_locs[target_th * (no_of_workers - 1)];
		}
		else
		{
			start = pivot_locs[target_th * (no_of_workers - 1) + i - 1];
			if (i == no_of_workers - 1)
				end = (target_th + 1) * size / no_of_workers;
			else
				end = pivot_locs[target_th * (no_of_workers - 1) + i];
		}
		printf("TH(%d) <- TH(%d) \t [%d] ... [%d]\n", i, target_th, start, end);
		for (int j = start; j < end; j++)
		{
			bucket[bucket_idx++] = intarr[j];
		}
	}
	qsort(bucket, bucket_idx, sizeof(unsigned int), compare);
	bucket_sizes[i] = bucket_idx;
	// printf("BK[%d]:\n", i);
	// for (int j = 0; j < bucket_idx; j++)
	// 	printf("%d\n", bucket[j]);
	return (void *)bucket;
}

int compare(const void *a, const void *b)
{
	return (*(unsigned int *)a > *(unsigned int *)b) ? 1 : ((*(unsigned int *)a == *(unsigned int *)b) ? 0 : -1);
}

int checking(unsigned int *list, long size)
{
	long i;
	printf("First : %d\n", list[0]);
	printf("At 25%%: %d\n", list[size / 4]);
	printf("At 50%%: %d\n", list[size / 2]);
	printf("At 75%%: %d\n", list[3 * size / 4]);
	printf("Last  : %d\n", list[size - 1]);
	for (i = 0; i < size - 1; i++)
	{
		if (list[i] > list[i + 1])
		{
			return 0;
		}
	}
	return 1;
}
