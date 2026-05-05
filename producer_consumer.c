#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

//Sleep and Wakeup - Producer-Consumer Problem

#define bufferSize 5 //buffer (queue) size //define N 100

char buffer[bufferSize]; 

int count, in, out, done; //int count = 0;

pthread_cond_t cProducer = PTHREAD_COND_INITIALIZER;
pthread_cond_t cConsumer = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void* producer(void* arg) //void producer(void)
{
    FILE* file = fopen("message.txt", "r");
    if (!file) 
    {
        perror("Failed to open message.txt");
        return NULL;
    }

    int item; //int item;

    while(1) // while(TRUE)
    {
        item = fgetc(file); //item = produce_item();
        if(item == EOF)
        {
            break;
        } 
        pthread_mutex_lock(&mutex);
        while(count == bufferSize) //if (count == N) sleep();
        {
            pthread_cond_wait(&cProducer, &mutex);
        }
        buffer[in] = item; //insert_item(item);
        in = (in + 1) % bufferSize;

        count++; //count = count + 1;

        if(count == 1) //if (count == 1) wakeup(consumer);
        {
            pthread_cond_signal(&cConsumer);
        }

        printf("Producer Inserted: %c\n", item);
        
        pthread_mutex_unlock(&mutex);
        usleep(50000); 
    }
    pthread_mutex_lock(&mutex);
    done = 1;
    pthread_cond_broadcast(&cConsumer);
    pthread_mutex_unlock(&mutex);

    fclose(file);
    return NULL;
}

void* consumer(void* arg) 
{
    int item; //int item;

    while(1) //while(TRUE)
    {
        pthread_mutex_lock(&mutex);
        while (count == 0 && !done) //if (count == 0) sleep();
        {
            pthread_cond_wait(&cConsumer, &mutex);
        }
        if(count == 0 && done) 
        {
            pthread_mutex_unlock(&mutex);
            break;
        }

        item = buffer[out]; //item = remove_item();
        out = (out + 1) % bufferSize;

        count--; //count = count - 1;

        if(count == bufferSize - 1) // if (count == N - 1) wakeup(producer);
        {
            pthread_cond_signal(&cProducer);
        }

        printf("Consumer Removed: %c\n", item); //consume_item(item);

        pthread_mutex_unlock(&mutex);
        usleep(150000); 
    }
    return NULL;
}

int main() 
{
    pthread_t tdidProducer, tdidConsumer;

    pthread_create(&tdidProducer, NULL, producer, NULL);
    pthread_create(&tdidConsumer, NULL, consumer, NULL);
    pthread_join(tdidProducer, NULL);
    pthread_join(tdidConsumer, NULL);

    printf("\nProducer-Consumer Algorithm completed.\n");

    return 0;
}