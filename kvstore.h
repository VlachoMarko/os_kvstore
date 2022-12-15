#ifndef KVSTORE_H
#define KVSTORE_H

#include "common.h"
#include "hash.h"

struct user_item {
    // Add your fields here.
    // You can access this structure from ht_item's user field defined in hash.h
    pthread_mutex_t mutex;
    pthread_cond_t cond_var;
};

struct user_ht {
    // Add your fields here.
    // You can access this structure from the hashtable_t's user field define in hash.h
};

#endif
