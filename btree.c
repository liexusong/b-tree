#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include "btree.h"

int maxHeight;
int nNodesIns;
int nNodesDel;
int nKeysIns;
int nKeysDel;
int nDiskReads;
int nDiskWrites;

int bErrLineNo;

static hNode hList;
static hNode *h;

#define error(rc) lineError(__LINE__, rc)

static bErrType lineError(int lineno, bErrType rc) {
    if (rc == bErrIO || rc == bErrMemory)
        if (!bErrLineNo)
            bErrLineNo = lineno;
    return rc;
}

static bAdrType allocAdr(void) {
    bAdrType adr;
    adr = h->nextFreeAdr;
    h->nextFreeAdr += h->sectorSize;
    return adr;
}

static bErrType flush(bufType *buf) {
    int len;

    len = h->sectorSize;
    if (buf->adr == 0) len *= 3;
    if (fseek(h->fp, buf->adr, SEEK_SET)) return error(bErrIO);
    if (fwrite(buf->p, len, 1, h->fp) != 1) return error(bErrIO);
    buf->modified = false;
    nDiskWrites++;
    return bErrOk;
}

static bErrType flushAll(void) {
    bErrType rc;
    bufType *buf;

    if (h->root.modified)
        if ((rc = flush(&h->root)) != 0) return rc;

    buf = h->bufList.next;
    while (buf != &h->bufList) {
        if (buf->modified)
            if ((rc = flush(buf)) != 0) return rc;
        buf = buf->next;
    }
    return bErrOk;
}

static bErrType assignBuf(bAdrType adr, bufType **b) {
    bufType *buf;
    bErrType rc;

    if (adr == 0) {
        *b = &h->root;
        return bErrOk;
    }

    buf = h->bufList.next;
    while (buf->next != &h->bufList) {
        if (buf->valid && buf->adr == adr) break;
        buf = buf->next;
    }

    if (buf->valid) {
        if (buf->adr != adr) {
            if (buf->modified) {
                if ((rc = flush(buf)) != 0) return rc;
            }
            buf->adr = adr;
            buf->valid = false;
        }
    } else {
        buf->adr = adr;
    }

    buf->next->prev = buf->prev;
    buf->prev->next = buf->next;
    buf->next = h->bufList.next;
    buf->prev = &h->bufList;
    buf->next->prev = buf;
    buf->prev->next = buf;
    *b = buf;
    return bErrOk;
}

static bErrType writeDisk(bufType *buf) {
    buf->valid = true;
    buf->modified = true;
    return bErrOk;
}

static bErrType readDisk(bAdrType adr, bufType **b) {
    int len;
    bufType *buf;
    bErrType rc;

    if ((rc = assignBuf(adr, &buf)) != 0) return rc;
    if (!buf->valid) {
        len = h->sectorSize;
        if (adr == 0) len *= 3;
        if (fseek(h->fp, adr, SEEK_SET)) return error(bErrIO);
        if (fread(buf->p, len, 1, h->fp) != 1) return error(bErrIO);
        buf->modified = false;
        buf->valid = true;
        nDiskReads++;
    }
    *b = buf;
    return bErrOk;
}

typedef enum { MODE_FIRST, MODE_MATCH } modeEnum;

static int search(bufType *buf, void *key, keyType **mkey, modeEnum mode) {
    int cc;
    int m;
    int lb;
    int ub;
    bool foundDup;

    foundDup = false;
    lb = 0;
    ub = ct(buf) - 1;
    while (lb <= ub) {
        m = (lb + ub) / 2;
        *mkey = fkey(buf) + ks(m);
        cc = h->comp(key, key(*mkey));
        if (cc < 0)
            ub = m - 1;
        else if (cc > 0)
            lb = m + 1;
        else {
            return cc;
        }
    }
    if (ct(buf) == 0) {
        *mkey = fkey(buf);
        return CC_LT;
    }

    return cc;
}

static bErrType scatterRoot(void) {
    bufType *gbuf;
    bufType *root;

    root = &h->root;
    gbuf = &h->gbuf;
    memcpy(fkey(root), fkey(gbuf), ks(ct(gbuf)));
    childLT(fkey(root)) = childLT(fkey(gbuf));
    ct(root) = ct(gbuf);
    leaf(root) = leaf(gbuf);
    return bErrOk;
}

static bErrType scatter(bufType *pbuf, keyType *pkey, int is, bufType **tmp) {
    bufType *gbuf;
    keyType *gkey;
    bErrType rc;
    int iu;
    int k0Min;
    int knMin;
    int k0Max;
    int knMax;
    int sw;
    int len;
    int base;
    int extra;
    int ct;
    int i;

    gbuf = &h->gbuf;
    gkey = fkey(gbuf);
    ct = ct(gbuf);

    iu = is;

    if (leaf(gbuf)) {
        k0Max= h->maxCt - 1;
        knMax= h->maxCt - 1;
        k0Min= (h->maxCt / 2) + 1;
        knMin= (h->maxCt / 2) + 1;
    } else {
        k0Max = h->maxCt - 1;
        knMax = h->maxCt;
        k0Min = (h->maxCt / 2) + 1;
        knMin = ((h->maxCt+1) / 2) + 1;
    }

    while(1) {
        if (iu == 0 || ct > (k0Max + (iu-1)*knMax)) {
            if ((rc = assignBuf(allocAdr(), &tmp[iu])) != 0)
                return rc;
            if (leaf(gbuf)) {
                if (iu == 0) {
                    prev(tmp[0]) = 0;
                    next(tmp[0]) = 0;
                } else {
                    prev(tmp[iu]) = tmp[iu-1]->adr;
                    next(tmp[iu]) = next(tmp[iu-1]);
                    next(tmp[iu-1]) = tmp[iu]->adr;
                }
            }
            iu++;
            nNodesIns++;
        } else if (iu > 1 && ct < (k0Min + (iu-1)*knMin)) {
            iu--;
            if (leaf(gbuf) && tmp[iu-1]->adr) {
                next(tmp[iu-1]) = next(tmp[iu]);
            }
            next(tmp[iu-1]) = next(tmp[iu]);
            nNodesDel++;
        } else {
            break;
        }
    }

    base = ct / iu;
    extra = ct % iu;
    for (i = 0; i < iu; i++) {
        int n;

        n = base;
        if (i && extra) {
            n++;
            extra--;
        }
        ct(tmp[i]) = n;
    }

    if (iu != is) {
        if (leaf(gbuf) && next(tmp[iu-1])) {
            bufType *buf;
            if ((rc = readDisk(next(tmp[iu-1]), &buf)) != 0) return rc;
            prev(buf) = tmp[iu-1]->adr;
            if ((rc = writeDisk(buf)) != 0) return rc;
        }
        sw = ks(iu - is);
        if (sw < 0) {
            len = ks(ct(pbuf)) - (pkey - fkey(pbuf)) + sw;
            memmove(pkey, pkey - sw, len);
        } else {
            len = ks(ct(pbuf)) - (pkey - fkey(pbuf));
            memmove(pkey + sw, pkey, len);
        }
        if (ct(pbuf))
            ct(pbuf) += iu - is;
        else
            ct(pbuf) += iu - is - 1;
    }

    for (i = 0; i < iu; i++) {
        if (leaf(gbuf)) {
            childLT(fkey(tmp[i])) = 0;
            if (i == 0) {
                childLT(pkey) = tmp[i]->adr;
            } else {
                memcpy(pkey, gkey, ks(1));
                childGE(pkey) = tmp[i]->adr;
                pkey += ks(1);
            }
        } else {
            if (i == 0) {
                childLT(fkey(tmp[i])) = childLT(gkey);
                childLT(pkey) = tmp[i]->adr;
            } else {
                childLT(fkey(tmp[i])) = childGE(gkey);
                memcpy(pkey, gkey, ks(1));
                childGE(pkey) = tmp[i]->adr;
                gkey += ks(1);
                pkey += ks(1);
                ct(tmp[i])--;
            }
        }

        memcpy(fkey(tmp[i]), gkey, ks(ct(tmp[i])));
        leaf(tmp[i]) = leaf(gbuf);

        gkey += ks(ct(tmp[i]));
    }
    leaf(pbuf) = false;

    if ((rc = writeDisk(pbuf)) != 0) return rc;
    for (i = 0; i < iu; i++)
        if ((rc = writeDisk(tmp[i])) != 0) return rc;

    return bErrOk;
}

static bErrType gatherRoot(void) {
    bufType *gbuf;
    bufType *root;

    root = &h->root;
    gbuf = &h->gbuf;
    memcpy(p(gbuf), root->p, 3 * h->sectorSize);
    leaf(gbuf) = leaf(root);
    ct(root) = 0;
    return bErrOk;
}

static bErrType gather(bufType *pbuf, keyType **pkey, bufType **tmp) {
    bErrType rc;
    bufType *gbuf;
    keyType *gkey;

    if (*pkey == lkey(pbuf))
        *pkey -= ks(1);
    if ((rc = readDisk(childLT(*pkey), &tmp[0])) != 0) return rc;
    if ((rc = readDisk(childGE(*pkey), &tmp[1])) != 0) return rc;
    if ((rc = readDisk(childGE(*pkey + ks(1)), &tmp[2])) != 0) return rc;

    gbuf = &h->gbuf;
    gkey = fkey(gbuf);

    childLT(gkey) = childLT(fkey(tmp[0]));
    memcpy(gkey, fkey(tmp[0]), ks(ct(tmp[0])));
    gkey += ks(ct(tmp[0]));
    ct(gbuf) = ct(tmp[0]);

    if (!leaf(tmp[1])) {
        memcpy(gkey, *pkey, ks(1));
        childGE(gkey) = childLT(fkey(tmp[1]));
        ct(gbuf)++;
        gkey += ks(1);
    }
    memcpy(gkey, fkey(tmp[1]), ks(ct(tmp[1])));
    gkey += ks(ct(tmp[1]));
    ct(gbuf) += ct(tmp[1]);

    if (!leaf(tmp[2])) {
        memcpy(gkey, *pkey+ks(1), ks(1));
        childGE(gkey) = childLT(fkey(tmp[2]));
        ct(gbuf)++;
        gkey += ks(1);
    }
    memcpy(gkey, fkey(tmp[2]), ks(ct(tmp[2])));
    ct(gbuf) += ct(tmp[2]);

    leaf(gbuf) = leaf(tmp[0]);

    return bErrOk;
}

bErrType bOpen(bOpenType info, bHandleType *handle) {
    bErrType rc;
    int bufCt;
    bufType *buf;
    int maxCt;
    bufType *root;
    int i;
    nodeType *p;

    if ((info.sectorSize < sizeof(hNode)) || (info.sectorSize % 4))
        return bErrSectorSize;

    maxCt = info.sectorSize - (sizeof(nodeType) - sizeof(keyType));
    maxCt /= sizeof(bAdrType) + info.keySize + sizeof(eAdrType);
    if (maxCt < 6) return bErrSectorSize;


    if ((h = malloc(sizeof(hNode))) == NULL) return error(bErrMemory);
    memset(h, 0, sizeof(hNode));
    h->keySize = info.keySize;
    h->sectorSize = info.sectorSize;
    h->comp = info.comp;


    h->ks = sizeof(bAdrType) + h->keySize + sizeof(eAdrType);
    h->maxCt = maxCt;

    bufCt = 7;
    if ((h->malloc1 = malloc(bufCt * sizeof(bufType))) == NULL)
        return error(bErrMemory);
    buf = h->malloc1;

    if ((h->malloc2 = malloc((bufCt+6) * h->sectorSize + 2 * h->ks)) == NULL)
        return error(bErrMemory);
    p = h->malloc2;


    h->bufList.next = buf;
    h->bufList.prev = buf + (bufCt - 1);
    for (i = 0; i < bufCt; i++) {
        buf->next = buf + 1;
        buf->prev = buf - 1;
        buf->modified = false;
        buf->valid = false;
        buf->p = p;
        p = (nodeType *)((char *)p + h->sectorSize);
        buf++;
    }
    h->bufList.next->prev = &h->bufList;
    h->bufList.prev->next = &h->bufList;

    root = &h->root;
    root->p = p;
    p = (nodeType *)((char *)p + 3*h->sectorSize);
    h->gbuf.p = p;

    h->curBuf = NULL;
    h->curKey = NULL;

    if ((h->fp = fopen(info.iName, "r+b")) != NULL) {
        if ((rc = readDisk(0, &root)) != 0) return rc;
        if (fseek(h->fp, 0, SEEK_END)) return error(bErrIO);
        if ((h->nextFreeAdr = ftell(h->fp)) == -1) return error(bErrIO);
    } else if ((h->fp = fopen(info.iName, "w+b")) != NULL) {
        memset(root->p, 0, 3*h->sectorSize);
        leaf(root) = 1;
        h->nextFreeAdr = 3 * h->sectorSize;
    } else {
        free(h);
        return bErrFileNotOpen;
    }

    if (hList.next) {
        h->prev = hList.next;
        h->next = &hList;
        h->prev->next = h;
        h->next->prev = h;
    } else {
        h->prev = h->next = &hList;
        hList.next = hList.prev = h;
    }

    *handle = h;
    return bErrOk;
}


bErrType bClose(bHandleType handle) {
    h = handle;
    if (h == NULL) return bErrOk;

    if (h->next) {
        h->next->prev = h->prev;
        h->prev->next = h->next;
    }

    if (h->fp) {
        flushAll();
        fclose(h->fp);
    }

    if (h->malloc2) free(h->malloc2);
    if (h->malloc1) free(h->malloc1);
    free(h);
    return bErrOk;
}

bErrType bFindKey(bHandleType handle, void *key, eAdrType *rec) {
    keyType *mkey;
    bufType *buf;
    bErrType rc;

    h = handle;
    buf = &h->root;

    while (1) {
        if (leaf(buf)) {
            if (search(buf, key, &mkey, MODE_FIRST) == 0) {
                *rec = rec(mkey);
                h->curBuf = buf; h->curKey = mkey;
                return bErrOk;
            } else {
                return bErrKeyNotFound;
            }
        } else {
            if (search(buf, key, &mkey, MODE_FIRST) < 0) {
                if ((rc = readDisk(childLT(mkey), &buf)) != 0) return rc;
            } else {
                if ((rc = readDisk(childGE(mkey), &buf)) != 0) return rc;
            }
        }
    }
}

bErrType bInsertKey(bHandleType handle, void *key, eAdrType rec) {
    int rc;
    keyType *mkey;
    int len;
    int cc;
    bufType *buf, *root;
    bufType *tmp[4];
    unsigned int keyOff;
    bool lastGEvalid;
    bool lastLTvalid;
    bAdrType lastGE;
    unsigned int lastGEkey;
    int height;

    h = handle;
    root = &h->root;
    lastGEvalid = false;
    lastLTvalid = false;

    if (ct(root) == 3 * h->maxCt) {
        if ((rc = gatherRoot()) != 0) return rc;
        if ((rc = scatter(root, fkey(root), 0, tmp)) != 0) return rc;
    }
    buf = root;
    height = 0;
    while(1) {
        if (leaf(buf)) {
            if (height > maxHeight) maxHeight = height;

            switch(search(buf, key, &mkey, MODE_MATCH)) {
            case CC_LT:
                if (h->comp(key, mkey) == CC_EQ)
                    return bErrDupKeys;
                break;
            case CC_EQ:
                return bErrDupKeys;
                break;
            case CC_GT:
                if (h->comp(key, mkey) == CC_EQ)
                    return bErrDupKeys;
                mkey += ks(1);
                break;
            }

            keyOff = mkey - fkey(buf);
            len = ks(ct(buf)) - keyOff;
            if (len) memmove(mkey + ks(1), mkey, len);

            memcpy(key(mkey), key, h->keySize);
            rec(mkey) = rec;
            childGE(mkey) = 0;
            ct(buf)++;
            if ((rc = writeDisk(buf)) != 0) return rc;
            if (!keyOff && lastLTvalid) {
                bufType *tbuf;
                keyType *tkey;
                if ((rc = readDisk(lastGE, &tbuf)) != 0) return rc;
                tkey = fkey(tbuf) + lastGEkey;
                memcpy(key(tkey), key, h->keySize);
                rec(tkey) = rec;
                if ((rc = writeDisk(tbuf)) != 0) return rc;
            }
            nKeysIns++;
            break;
        } else {
            bufType *cbuf;
            height++;

            if ((cc = search(buf, key, &mkey, MODE_MATCH)) < 0) {
                if ((rc = readDisk(childLT(mkey), &cbuf)) != 0) return rc;
            } else {
                if ((rc = readDisk(childGE(mkey), &cbuf)) != 0) return rc;
            }

            if (ct(cbuf) == h->maxCt) {
                if ((rc = gather(buf, &mkey, tmp)) != 0) return rc;
                if ((rc = scatter(buf, mkey, 3, tmp)) != 0) return rc;

                if ((cc = search(buf, key, &mkey, MODE_MATCH)) < 0) {
                    if ((rc = readDisk(childLT(mkey), &cbuf)) != 0) return rc;
                } else {
                    if ((rc = readDisk(childGE(mkey), &cbuf)) != 0) return rc;
                }
            }
            if (cc >= 0 || mkey != fkey(buf)) {
                lastGEvalid = true;
                lastLTvalid = false;
                lastGE = buf->adr;
                lastGEkey = mkey - fkey(buf);
                if (cc < 0) lastGEkey -= ks(1);
            } else {
                if (lastGEvalid) lastLTvalid = true;
            }
            buf = cbuf;
        }
    }

    return bErrOk;
}

bErrType bDeleteKey(bHandleType handle, void *key) {
    int rc;
    keyType *mkey;
    int len;
    int cc;
    bufType *buf;
    bufType *tmp[4];
    unsigned int keyOff;
    bool lastGEvalid;
    bool lastLTvalid;
    bAdrType lastGE;
    unsigned int lastGEkey;
    bufType *root;
    bufType *gbuf;

    h = handle;
    root = &h->root;
    gbuf = &h->gbuf;
    lastGEvalid = false;
    lastLTvalid = false;

    buf = root;
    while(1) {
        if (leaf(buf)) {
            if (search(buf, key, &mkey, MODE_MATCH) != 0)
                return bErrKeyNotFound;

            keyOff = mkey - fkey(buf);
            len = ks(ct(buf)-1) - keyOff;
            if (len) memmove(mkey, mkey + ks(1), len);
            ct(buf)--;
            if ((rc = writeDisk(buf)) != 0) return rc;
            if (!keyOff && lastLTvalid) {
                bufType *tbuf;
                keyType *tkey;
                if ((rc = readDisk(lastGE, &tbuf)) != 0) return rc;
                tkey = fkey(tbuf) + lastGEkey;
                memcpy(key(tkey), mkey, h->keySize);
                rec(tkey) = rec(mkey);
                if ((rc = writeDisk(tbuf)) != 0) return rc;
            }
            nKeysDel++;
            break;
        } else {
            bufType *cbuf;

            if ((cc = search(buf, key, &mkey, MODE_MATCH)) < 0) {
                if ((rc = readDisk(childLT(mkey), &cbuf)) != 0) return rc;
            } else {
                if ((rc = readDisk(childGE(mkey), &cbuf)) != 0) return rc;
            }

            if (ct(cbuf) == h->maxCt/2) {
                if ((rc = gather(buf, &mkey, tmp)) != 0) return rc;
                if (buf == root && ct(root) == 2 && ct(gbuf) < (3*(3*h->maxCt))/4) {
                    scatterRoot();
                    nNodesDel += 3;
                    continue;
                }

                if ((rc = scatter(buf, mkey, 3, tmp)) != 0) return rc;
                if ((cc = search(buf, key, &mkey, MODE_MATCH)) < 0) {
                    if ((rc = readDisk(childLT(mkey), &cbuf)) != 0) return rc;
                } else {
                    if ((rc = readDisk(childGE(mkey), &cbuf)) != 0) return rc;
                }
            }
            if (cc >= 0 || mkey != fkey(buf)) {
                lastGEvalid = true;
                lastLTvalid = false;
                lastGE = buf->adr;
                lastGEkey = mkey - fkey(buf);
                if (cc < 0) lastGEkey -= ks(1);
            } else {
                if (lastGEvalid) lastLTvalid = true;
            }
            buf = cbuf;
        }
    }

    return bErrOk;
}

bErrType bFindFirstKey(bHandleType handle, void *key, eAdrType *rec) {
    bErrType rc;
    bufType *buf;

    h = handle;
    buf = &h->root;
    while (!leaf(buf)) {
        if ((rc = readDisk(childLT(fkey(buf)), &buf)) != 0) return rc;
    }
    if (ct(buf) == 0) return bErrKeyNotFound;
    memcpy(key, key(fkey(buf)), h->keySize);
    *rec = rec(fkey(buf));
    h->curBuf = buf; h->curKey = fkey(buf);
    return bErrOk;
}

bErrType bFindLastKey(bHandleType handle, void *key, eAdrType *rec) {
    bErrType rc;
    bufType *buf;

    h = handle;
    buf = &h->root;
    while (!leaf(buf)) {
        if ((rc = readDisk(childGE(lkey(buf)), &buf)) != 0) return rc;
    }
    if (ct(buf) == 0) return bErrKeyNotFound;
    memcpy(key, key(lkey(buf)), h->keySize);
    *rec = rec(lkey(buf));
    h->curBuf = buf; h->curKey = lkey(buf);
    return bErrOk;
}

bErrType bFindNextKey(bHandleType handle, void *key, eAdrType *rec) {
    bErrType rc;
    keyType *nkey;
    bufType *buf;

    h = handle;
    if ((buf = h->curBuf) == NULL) return bErrKeyNotFound;
    if (h->curKey == lkey(buf)) {
        if (next(buf)) {
            if ((rc = readDisk(next(buf), &buf)) != 0) return rc;
            nkey = fkey(buf);
        } else {
            return bErrKeyNotFound;
        }
    } else {
        nkey = h->curKey + ks(1);
    }
    memcpy(key, key(nkey), h->keySize);
    *rec = rec(nkey);
    h->curBuf = buf; h->curKey = nkey;
    return bErrOk;
}

bErrType bFindPrevKey(bHandleType handle, void *key, eAdrType *rec) {
    bErrType rc;
    keyType *pkey;
    keyType *fkey;
    bufType *buf;

    h = handle;
    if ((buf = h->curBuf) == NULL) return bErrKeyNotFound;
    fkey = fkey(buf);
    if (h->curKey == fkey) {
        if (prev(buf)) {
            if ((rc = readDisk(prev(buf), &buf)) != 0) return rc;
            pkey = fkey(buf) + ks((ct(buf) - 1));
        } else {

            return bErrKeyNotFound;
        }
    } else {

        pkey = h->curKey - ks(1);
    }
    memcpy(key, key(pkey), h->keySize);
    *rec = rec(pkey);
    h->curBuf = buf; h->curKey = pkey;
    return bErrOk;
}
