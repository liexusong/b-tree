#ifndef BTREE_H
#define BTREE_H

typedef long eAdrType;
typedef long bAdrType;

#define CC_EQ           0
#define CC_GT           1
#define CC_LT          -1

typedef int (*bCompType)(const void *key1, const void *key2);

typedef enum {false, true} bool;
typedef enum {
    bErrOk,
    bErrKeyNotFound,
    bErrDupKeys,
    bErrSectorSize,
    bErrFileNotOpen,
    bErrFileExists,
    bErrIO,
    bErrMemory,
} bErrType;

typedef void *bHandleType;

typedef struct {
    char *iName;
    int keySize;
    int sectorSize;
    bCompType comp;
} bOpenType;

#define bAdr(p) *(bAdrType *)(p)
#define eAdr(p) *(eAdrType *)(p)

#define childLT(k) bAdr((char *)k - sizeof(bAdrType))
#define key(k) (k)
#define rec(k) eAdr((char *)(k) + h->keySize)
#define childGE(k) bAdr((char *)(k) + h->keySize + sizeof(eAdrType))

#define leaf(b) b->p->leaf
#define ct(b) b->p->ct
#define next(b) b->p->next
#define prev(b) b->p->prev
#define fkey(b) &b->p->fkey
#define lkey(b) (fkey(b) + ks((ct(b) - 1)))
#define p(b) (char *)(b->p)

#define ks(ct) ((ct) * h->ks)

typedef char keyType;

typedef struct {
    unsigned int leaf:1;
    unsigned int ct:15;
    bAdrType prev;
    bAdrType next;
    bAdrType childLT;
    keyType fkey;
} nodeType;

typedef struct bufTypeTag {
    struct bufTypeTag *next;
    struct bufTypeTag *prev;
    bAdrType adr;
    nodeType *p;
    bool valid;
    bool modified;
} bufType;

typedef struct hNodeTag {
    struct hNodeTag *prev;
    struct hNodeTag *next;
    FILE *fp;
    int keySize;
    int sectorSize;
    bCompType comp;
    bufType root;
    bufType bufList;
    void *malloc1;
    void *malloc2;
    bufType gbuf;
    bufType *curBuf;
    keyType *curKey;
    unsigned int maxCt;
    int ks;
    bAdrType nextFreeAdr;
} hNode;

bErrType bOpen(bOpenType info, bHandleType *handle);
bErrType bClose(bHandleType handle);
bErrType bInsertKey(bHandleType handle, void *key, eAdrType rec);
bErrType bDeleteKey(bHandleType handle, void *key);
bErrType bFindKey(bHandleType handle, void *key, eAdrType *rec);
bErrType bFindFirstKey(bHandleType handle, void *key, eAdrType *rec);
bErrType bFindLastKey(bHandleType handle, void *key, eAdrType *rec);
bErrType bFindNextKey(bHandleType handle, void *key, eAdrType *rec);
bErrType bFindPrevKey(bHandleType handle, void *key, eAdrType *rec);

#endif
