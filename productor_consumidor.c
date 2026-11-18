#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define N                5      /* Capacidad del buffer */
#define NUM_PRODUCTORES  2
#define NUM_CONSUMIDORES 2
#define ITEMS_POR_HILO   10     /* Elementos que produce o consume cada hilo */
#define MAX_ESPERA_US    200000 /* Pausa aleatoria maxima: 0.2 s */

#ifdef __APPLE__
#include <dispatch/dispatch.h>
typedef dispatch_semaphore_t semaforo_t;
static void sem_crear(semaforo_t *s, unsigned int valor) { *s = dispatch_semaphore_create(valor); }
static void P(semaforo_t *s) { dispatch_semaphore_wait(*s, DISPATCH_TIME_FOREVER); }
static void V(semaforo_t *s) { dispatch_semaphore_signal(*s); }
static void sem_eliminar(semaforo_t *s) { dispatch_release(*s); }
#else
#include <semaphore.h>
typedef sem_t semaforo_t;
static void sem_crear(semaforo_t *s, unsigned int valor)
{
    /* El segundo argumento es 0: compartido solo entre hilos del proceso */
    if (sem_init(s, 0, valor) != 0) {
        perror("sem_init");
        exit(EXIT_FAILURE);
    }
}
static void P(semaforo_t *s) { sem_wait(s); }
static void V(semaforo_t *s) { sem_post(s); }
static void sem_eliminar(semaforo_t *s) { sem_destroy(s); }
#endif

/* Variables compartidas */
int buf[N];
int front = 0, rear = 0;
semaforo_t empty, full, mutexP, mutexC;

long suma_producida = 0; /* Protegida por mutexP */
long suma_consumida = 0; /* Protegida por mutexC */

void *productor(void *arg)
{
    int id = *(int *)arg;
    unsigned int semilla = (unsigned int)time(NULL) ^ (unsigned int)(id * 7919);

    for (int i = 1; i <= ITEMS_POR_HILO; i++) {
        /* Producir dato: el valor identifica al productor y al numero de item */
        int data = id * 100 + i;
        usleep(rand_r(&semilla) % MAX_ESPERA_US);

        P(&empty);   /* Espera un espacio vacio */
        P(&mutexP);  /* Entra a la seccion critica de productores */

        buf[rear] = data;
        printf("Productor %d deposita %3d en buf[%d]\n", id, data, rear);
        rear = (rear + 1) % N;
        suma_producida += data;

        V(&mutexP);  /* Sale de la seccion critica */
        V(&full);    /* Avisa que hay un dato disponible */
    }

    pthread_exit(NULL);
}

void *consumidor(void *arg)
{
    int id = *(int *)arg;
    unsigned int semilla = (unsigned int)time(NULL) ^ (unsigned int)(id * 104729);

    for (int i = 1; i <= ITEMS_POR_HILO; i++) {
        int result;

        P(&full);    /* Espera un dato disponible */
        P(&mutexC);  /* Entra a la seccion critica de consumidores */

        result = buf[front];
        printf("    Consumidor %d extrae %3d de buf[%d]\n", id, result, front);
        front = (front + 1) % N;
        suma_consumida += result;

        V(&mutexC);  /* Sale de la seccion critica */
        V(&empty);   /* Avisa que hay un espacio vacio */

        /* Consumir dato */
        usleep(rand_r(&semilla) % MAX_ESPERA_US);
    }

    pthread_exit(NULL);
}

int main(void)
{
    pthread_t productores[NUM_PRODUCTORES], consumidores[NUM_CONSUMIDORES];
    int id_productor[NUM_PRODUCTORES], id_consumidor[NUM_CONSUMIDORES];

    sem_crear(&empty, N);
    sem_crear(&full, 0);
    sem_crear(&mutexP, 1);
    sem_crear(&mutexC, 1);

    printf("Productor-consumidor: %d productores, %d consumidores, buffer de %d espacios\n\n",
           NUM_PRODUCTORES, NUM_CONSUMIDORES, N);

    for (int i = 0; i < NUM_PRODUCTORES; i++) {
        id_productor[i] = i + 1;
        if (pthread_create(&productores[i], NULL, productor, &id_productor[i]) != 0) {
            perror("pthread_create");
            return EXIT_FAILURE;
        }
    }
    for (int i = 0; i < NUM_CONSUMIDORES; i++) {
        id_consumidor[i] = i + 1;
        if (pthread_create(&consumidores[i], NULL, consumidor, &id_consumidor[i]) != 0) {
            perror("pthread_create");
            return EXIT_FAILURE;
        }
    }

    for (int i = 0; i < NUM_PRODUCTORES; i++)
        pthread_join(productores[i], NULL);
    for (int i = 0; i < NUM_CONSUMIDORES; i++)
        pthread_join(consumidores[i], NULL);

    printf("\nElementos producidos: %d | suma: %ld\n",
           NUM_PRODUCTORES * ITEMS_POR_HILO, suma_producida);
    printf("Elementos consumidos: %d | suma: %ld\n",
           NUM_CONSUMIDORES * ITEMS_POR_HILO, suma_consumida);

    printf("\nVerificacion:\n");

    if (suma_producida == suma_consumida) {
        printf("Correcto: todos los elementos fueron consumidos.\n");
    } else {
        printf("Error: las sumas no coinciden.\n");
    }

    printf("Todos los hilos han terminado.\n");

    sem_eliminar(&empty);
    sem_eliminar(&full);
    sem_eliminar(&mutexP);
    sem_eliminar(&mutexC);

    return EXIT_SUCCESS;
}
