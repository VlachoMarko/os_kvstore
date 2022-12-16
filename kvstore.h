#ifndef KVSTORE_H
#define KVSTORE_H

#include "common.h"
#include "hash.h"

struct user_item {
    // Add your fields here.
    // You can access this structure from ht_item's user field defined in hash.h
    pthread_rwlock_t rwlock;
};

struct user_ht {
    // Add your fields here.
    // You can access this structure from the hashtable_t's user field define in hash.h
};

#endif
