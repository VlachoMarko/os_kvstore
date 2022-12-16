#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include <assert.h>
#include <pthread.h>

#include "server_utils.h"
#include "common.h"
#include "request_dispatcher.h"
#include "hash.h"
#include "kvstore.h"

// DO NOT MODIFY THIS.
// ./check.py assumes the hashtable has 256 buckets.
// You should initialize your hashtable with this capacity.
#define HT_CAPACITY 256
int used_ids[100] = { };

int ht_init(){
    ht = malloc(sizeof(hashtable_t));

    if(!ht){
        return -1;
    }

    ht->items = calloc(HT_CAPACITY, sizeof(hash_item_t *));
    
    if(ht->items == NULL){
        return -1;
    }
    ht->capacity = HT_CAPACITY;
    ht->user = (void *)malloc(sizeof(struct user_ht));
    return 0;
}

int handle_errors(int socket, struct request *request, int res){
    switch(res){
        
        case 1:
            send_response(socket, KEY_ERROR, 0, NULL);
            break;
        
        case 2:
            send_response(socket, PARSING_ERROR, 0, NULL);
            break;
        
        case 3:
            send_response(socket, STORE_ERROR, 0, NULL);
            request->connection_close = 1;
            break;
        
        case 4:
            send_response(socket, SETOPT_ERROR, 0, NULL);
            break;
        
        case 5:
            send_response(socket, UNK_ERROR, 0, NULL);
            break;  
        
        default:
            pr_info("undefined error code");
    }

    return 0;
}

int rst_request(){
    free(ht);
    if(ht_init() != 0){
        pr_info("reset error");
        return -1;
    };
    return 0;
}

int del_request(int socket, struct request *request){

    hash_item_t *item = NULL;
    char *key = request->key;

    unsigned int h = hash(key) % ht->capacity;
    hash_item_t *e = ht->items[h];

    while (e!=NULL){
        if(strcmp(e->key, key) == 0){
            item = e;
            break;
        }
        e = e->next;
    }

    if(item == NULL){
        return 1;   
    }

    pthread_rwlock_t *rwlock_ptr = &item->user->rwlock;
    if(pthread_rwlock_trywrlock(rwlock_ptr) != 0){
        pr_info("1readlock error %p\n", rwlock_ptr);
        return 1;
    }
    else {
        pr_info("1rwlock: %p writelocked\n", rwlock_ptr);
    }


    if(item->next != NULL && item->prev != NULL){

        if(pthread_rwlock_trywrlock(&item->prev->user->rwlock) != 0){
            pr_info("2writelock error %p\n", &item->prev->user->rwlock);
            return 1;
        }
        else {
            pr_info("2rwlock: %p writelocked\n", &item->prev->user->rwlock);
        }

        if(pthread_rwlock_trywrlock(&item->next->user->rwlock) != 0){
            pr_info("3writelock error %p\n", &item->next->user->rwlock);
            return 1;
        }
        else {
            pr_info("3rwlock: %p writelocked\n", &item->next->user->rwlock);
        }

        hash_item_t *prevItem = item->prev;
        hash_item_t *nextItem = item->next;
        
        prevItem->next = nextItem;
        nextItem->prev = prevItem;

        if(pthread_rwlock_unlock(&item->next->user->rwlock) != 0){
            pr_info("4unlock error %p\n", &item->next->user->rwlock); 
            return 3;   
        }
        else {
            pr_info("4rwlock: %p unlocked\n", &item->next->user->rwlock);
        }
        
        if(pthread_rwlock_unlock(&item->prev->user->rwlock) != 0){
            pr_info("5unlock error %p\n", &item->prev->user->rwlock); 
        
            return 3;   
        }
        else {
            pr_info("5rwlock: %p unlocked\n", &item->prev->user->rwlock);
        }

    }
    else if(item->prev != NULL){

        pthread_rwlock_t *rwlock_ptr1 = &item->prev->user->rwlock;
        
        if(pthread_rwlock_trywrlock(rwlock_ptr1) != 0){
            pr_info("6writelock error %p\n", rwlock_ptr1);
            return 1;
        }
        else {
            pr_info("6rwlock: %p writelocked\n", rwlock_ptr1);
        }

        hash_item_t *prevItem = item->prev;
        prevItem->next = NULL;

        if(pthread_rwlock_unlock(rwlock_ptr1) != 0){
        pr_info("7unlock error %p\n", rwlock_ptr1); 
        
        return 3;   
        }
        else {
            pr_info("7rwlock: %p unlocked\n", rwlock_ptr1);
        }
    }
    else if(item->next != NULL){

        pthread_rwlock_t *rwlock_ptr1 = &item->next->user->rwlock;
        if(pthread_rwlock_trywrlock(rwlock_ptr1) != 0){
            pr_info("8writelock error %p\n", rwlock_ptr1);
            return 1;
        }
        else {
            pr_info("8rwlock: %p writelocked\n", rwlock_ptr1);
        }

        ht->items[h] = ht->items[h]->next;
        ht->items[h]->prev = NULL;

        if(pthread_rwlock_unlock(rwlock_ptr1) != 0){
            pr_info("9unlock error %p\n", rwlock_ptr1); 
            
            return 3;   
        }
        else {
            pr_info("9rwlock: %p unlocked\n", rwlock_ptr1);
        }
    }
    else {
        ht->items[h] = NULL;
    }

    send_response(socket, OK, 0, NULL);
    
    if(pthread_rwlock_unlock(rwlock_ptr) != 0){
        pr_info("10unlock error %p\n", rwlock_ptr); 
        
        return 3;   
    }
    else {
        pr_info("10rwlock: %p unlocked\n", rwlock_ptr);
    }
    

    return 0;
}

int get_request(int socket, struct request *request){

    hash_item_t *item = NULL;
    char *key = request->key;

    unsigned int h = hash(key) % ht->capacity;
    hash_item_t *e = ht->items[h];

    while (e!=NULL){
        if(strcmp(e->key, key) == 0){
            item = e;
            break;
        }
        e = e->next;
    }

    
    if(item == NULL){
        return 1;   
    }
    
    if(pthread_rwlock_tryrdlock(&item->user->rwlock) != 0){
        pr_info("1writelock error %p\n", &item->user->rwlock);
        return 1;
    }
    else {
        pr_info("1rwlock: %p writelocked\n", &item->user->rwlock);
    }


    unsigned int res_size = item->value_size;
    char *resbuf = malloc(res_size+1);

    memcpy(resbuf, item->value, res_size);

    strcpy(resbuf+res_size, "\0");
    send_response(socket, OK, res_size, resbuf);
    
    if(pthread_rwlock_unlock(&item->user->rwlock) != 0){
        pr_info("2unlock error %p\n", &item->user->rwlock); 
        
        return 3;   
    }
    else {
        pr_info("2rwlock: %p unlocked\n", &item->user->rwlock);
    }


    return 0;
}

int set_request(int socket, struct request *request)
{
    size_t len = 0;
    size_t expected_len = request->msg_len;
    char rcvbuf[expected_len+1];
    int chunk_size = expected_len;
    
    hash_item_t *item = NULL;
    char *key = request->key;
    // 1. Lock the hashtable entry. Create it if the key is not in the store.

    unsigned int h = hash(key) % ht->capacity;
    pr_info("bucket i: %u\n", h);
    hash_item_t *e = ht->items[h];

    //Find the item
    while (e!=NULL){
        if(strcmp(e->key, key) == 0){
            item = e;
            break;
        }
        e = e->next;
    }

    if(item == NULL){

        hash_item_t *new_item = malloc(sizeof(hash_item_t));
        new_item->user = (void *)malloc(sizeof(struct user_item));
        new_item->prev = NULL;
        new_item->next = NULL;
        new_item->value = NULL;
        new_item->value_size = 0;

        new_item->key = malloc(strlen(key) + 1);
        strcpy(new_item->key, key);

        
        if(ht->items[h] != NULL){
            if(ht->items[h]->next == NULL){
                new_item->prev = ht->items[h];
                ht->items[h]->next = new_item;
            }
            else {

                e = ht->items[h]->next;
                hash_item_t *new_place = e;

                while (e!=NULL){
                    if(e->next == NULL){
                        new_place = e;
                        break;
                    }
                    e = e->next;
                }

                new_item->prev = new_place;
                new_place->next = new_item;
            }
        }
        else {
            ht->items[h] = malloc(sizeof(hash_item_t));
            ht->items[h]->user = (void *)malloc(sizeof(struct user_item));
            ht->items[h]->prev = NULL;
            ht->items[h]->next = NULL;
            ht->items[h]->value = NULL;
            ht->items[h]->value_size = 0;
            ht->items[h]->key = malloc(strlen(key) + 1);

            if(pthread_rwlock_init(&ht->items[h]->user->rwlock, NULL) != 0){
                pr_info("init error");
                return 3;
            }

            pthread_rwlock_t *head_rwlock = &ht->items[h]->user->rwlock;   
            
            if(pthread_rwlock_trywrlock(head_rwlock) != 0){
                pr_info("3writelock error %p\n", head_rwlock);
                read_payload(socket, request, chunk_size, rcvbuf);
                
                if(check_payload(socket, request, expected_len) != 0){
                    pr_info("payload error");
                    return 2;
                }
                return 1;
            }
            else {
                pr_info("3rwlock: %p writelocked\n", head_rwlock);
            }
            
            
            ht->items[h] = new_item;

            if(pthread_rwlock_unlock(head_rwlock) != 0){
                pr_info("4unlock error %p\n", head_rwlock);  
            }
            else {
                pr_info("4rwlock: %p unlocked\n", head_rwlock);
            }

        }

        if(pthread_rwlock_init(&new_item->user->rwlock, NULL) != 0){
            pr_info("init error");
            return 3;
        }

        item = new_item;
    }
    
    if(pthread_rwlock_trywrlock(&item->user->rwlock) != 0){
        pr_info("5lock error %p\n", &item->user->rwlock);
        // pthread_cond_wait(&item->user->cond_var, &item->user->rwlock);
        read_payload(socket, request, chunk_size, rcvbuf);
        if(check_payload(socket, request, expected_len) != 0){
            pr_info("payload error");
            return 2;
        }
        return 1;
    }
    else {
        pr_info("5rwlock: %p locked\n", &item->user->rwlock);
    }


    while (len < expected_len){

        // 2. Read the payload from the socket
        // Note: Clients may send a partial chunk of the payload so you should not wait
        // for the full data to be available before write in the hashtable entry.
 
        int rcved = read_payload(socket, request, chunk_size, rcvbuf);

        if(rcved != chunk_size){
            pr_info("Bad size\n");
            return 3;
        }

        len+=rcved;

        if(item->value == NULL && item->value_size == 0){
            item->value = malloc(expected_len+1);
            item->value[expected_len] = '\0';
        }
        else {
            free(item->value);
            item->value = malloc(expected_len+1);

            if(item->value == NULL){

                if(pthread_rwlock_unlock(&item->user->rwlock) != 0){
                    pr_info("7unlock error \n%p", &item->user->rwlock);  
                }
                else {
                    pr_info("7rwlock: %p unlocked\n", &item->user->rwlock);
                }
                return 3;
            }

            item->value[expected_len] = '\0';
        }
            
            memcpy(item->value, rcvbuf, expected_len);
            item->value_size = expected_len;
    }

    // 3. Write the partial payload on the entry

    // 4. Unlock the entry in the store to finalize the insertion.
    // This allow other threads to read the entry.

    // It checks if the payload has been fully received .
    // It also reads the last char of the request which should be '\n'
    
    if(check_payload(socket, request, expected_len) != 0){
        
        if(pthread_rwlock_unlock(&item->user->rwlock) != 0){
            pr_info("8unlock error %p\n", &item->user->rwlock);  
        }
        else {
            pr_info("8rwlock: %p unlocked\n", &item->user->rwlock);
        }

        return 3;
    }
        
    if(strcmp(ht->items[h]->key, item->key) == 0){
                
        ht->items[h] = item;
    }
    
    if(pthread_rwlock_unlock(&item->user->rwlock) != 0){
        pr_info("10unlock error %p\n", &item->user->rwlock); 
        return 3;   
    }
    else {
        pr_info("10rwlock: %p unlocked\n", &item->user->rwlock);
    }
        
    // Optionally you can close the connection
    // You should do it ONLY on errors:
    // request->connection_close = 1;
    send_response(socket, OK, 0, NULL);
    // pthread_cond_broadcast(&item->user->cond_var);
    return 0;
}

void *main_job(void *arg)
{   
    int method;
   
    struct conn_info *conn_info = arg;
    struct request *request = allocate_request();
    request->connection_close = 0;

    pr_info("Starting new session from %s:%d\n",
        inet_ntoa(conn_info->addr.sin_addr),
        ntohs(conn_info->addr.sin_port));


    int res = 5;
    do {
        method = recv_request(conn_info->socket_fd, request);
        // Insert your operations here
        switch (method) {
        case SET:
            res = set_request(conn_info->socket_fd, request);
            
            if(res > 0){
                handle_errors(conn_info->socket_fd, request, res);
            }
            break;
        case GET:
            res = get_request(conn_info->socket_fd, request);
            if(res > 0){
                handle_errors(conn_info->socket_fd, request, res);
            }
            break;
        case DEL:
            res = del_request(conn_info->socket_fd, request);
            if(res > 0){
                handle_errors(conn_info->socket_fd, request, res);
            }
            break;
        case RST:
            // ./check.py issues a reset request after each test
            // to bring back the hashtable to a known state.
            // Implement your reset command here.
            res = rst_request();
            if(res < 0){
                request->connection_close = 1;
            }
            send_response(conn_info->socket_fd, OK, 0, NULL);
            break;
        case STAT:
            res = 0;
            break;
        }

        if (request->key) {
            free(request->key);
        }

    } while (!request->connection_close);

    pr_info("closing connection\n");
    close_connection(conn_info->socket_fd);

    free(request);
    free(conn_info);
    pthread_exit((void *)NULL);
    return (void *)NULL;
}


unsigned int get_id(){

    unsigned int i = 0;

    for(;;){
        
        if(used_ids[i] == 0){
            pr_info("free index: %i\n", i);
            used_ids[i] = 1;
            return i;
        }
        
        i++;
    
        if(i==100) {
            pr_info("all threads used");
            i=0;
        }
    }
}


int main(int argc, char *argv[])
{
    int listen_sock;
    pthread_t threads[100];

    listen_sock = server_init(argc, argv);

    if(ht_init() != 0) {
        perror("Hashtable init error \n");
        exit(1);
    }

    // Initialize your hashtable.
    // @see kvstore.h for hashtable struct declaration

    for (;;) {

        // pr_info("loopbeg\n");
        struct conn_info *conn_info =
            calloc(1, sizeof(struct conn_info));
        if (accept_new_connection(listen_sock, conn_info) < 0) {
            error("Cannot accept new connection");
            free(conn_info);
            continue;
        }
        
        unsigned int index = get_id(threads);
        pthread_create(&threads[index], NULL , main_job, (struct conn_info *)conn_info);
        
        // pr_info("loopend\n");

    }

    return 0;
}
