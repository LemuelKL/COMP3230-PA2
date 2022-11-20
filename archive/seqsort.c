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

int checking(unsigned int *, long);
int compare(const void *, const void *);
int ssort(unsigned int *, long);

// global variables
long size;			  // size of the array
unsigned int *intarr; // array of random integers

int main(int argc, char **argv)
{
	long i, j;
	struct timeval start, end;

	if ((argc != 2))
	{
		printf("Usage: seq_sort <number>\n");
		exit(0);
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
	{
		intarr[i] = random();
	}

	// measure the start time
	gettimeofday(&start, NULL);

	// qsort(intarr, size, sizeof(unsigned int), compare);
	ssort(intarr, 4);

	// measure the end time
	gettimeofday(&end, NULL);

	if (!checking(intarr, size))
	{
		printf("The array is not in sorted order!!\n");
	}

	printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec) * 1.0 + (end.tv_usec - start.tv_usec) / 1000000.0);

	free(intarr);
	return 0;
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

int ssort(unsigned int *list, long b)
{
	// 1. choose b samples from list
	int *samples = (int *)malloc(b * sizeof(int));
	for (int i = 0; i < b; i++)
	{
		samples[i] = list[i];
	}
	// 2. sort the samples
	qsort(samples, b, sizeof(int), compare);
	// 3. partition the list into b + 1 buckets
	int buckets[b + 1][size];
	int *bucket_idx = (int *)malloc((b + 1) * sizeof(int));
	for (int i = 0; i < b + 1; i++)
	{
		bucket_idx[i] = 0;
	}
	for (int i = 0; i < size; i++)
	{
		int placed = 0;
		int ele = list[i];
		for (int j = 0; j < b + 1; j++)
		{
			int buck = samples[j];
			if (ele <= buck)
			{
				buckets[j][bucket_idx[j]] = ele;
				bucket_idx[j]++;
				placed = 1;
				break;
			}
		}
		if (!placed)
		{
			buckets[b][bucket_idx[b]] = ele;
			bucket_idx[b]++;
		}
	}
	// 4. sort each bucket
	for (int i = 0; i < b + 1; i++)
	{
		qsort(buckets[i], bucket_idx[i], sizeof(unsigned int), compare);
	}
	// print buckets for debug
	// for (int i = 0; i < b + 1; i++)
	// {
	// 	printf("bucket %d [%d]: ", i, samples[i]);
	// 	for (int j = 0; j < bucket_idx[i]; j++)
	// 	{
	// 		printf("%d ", buckets[i][j]);
	// 	}
	// 	printf("\n");
	// }
	// 5. merge the buckets
	int idx = 0;
	for (int i = 0; i < b + 1; i++)
	{
		for (int j = 0; j < bucket_idx[i]; j++)
		{
			list[idx] = buckets[i][j];
			idx++;
		}
	}
	return 0;
}