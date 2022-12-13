/* -------------------------------------------------------------------------
 *
 * glt.h
 * openGauss global logic time method definitions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/glt.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GLT_H
#define GLT_H

#include <map>
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "access/xact.h"
#include "access/twophase.h"

using namespace std;

extern const TransactionId GLT_ERROR_XID ; 

typedef struct GLTMethods {
    /* ------------------------------------------------------------------------
     * Global Logic Time APIs
     * ------------------------------------------------------------------------
     */

    /*
     * get GLobal CSN for SetXact2CommitInProgress, shardingsphere will send
     * the global CSN for commit before local transactions commit in distributed
     * transaction. GLT will compare global csn and nextCommitSeqNo (local csn)
     * , use the latest one.
     */
    CommitSeqNo (*getCommitCSNBeforeCommitInProgress)(CommitSeqNo csn, TransactionId xid);

    /*
     * get GLobal CSN to commit local transaction and advance local csn,
     * shardingsphere will send the global CSN before local commit starts in
     * distributed transacion. GLT will compare global csn and nextCommitSeqNo
     * (local csn), use the latest one. GLT won't advance local csn if
     * nextCommitSeqNo > global csn, because GLT hasn't been able to actively
     * advance the global csn.
     *
     * @param xid: xid of the transaction to be committed
     */
    void (*getCommitCSNInXact)(TransactionId xid);

    /*
     * get GLobal CSN to perfrom a twophase commit and advance local csn ,
     * shardingsphere will send the global CSN before local commit starts in
     * distributed transacion. GLT will compare global csn and nextCommitSeqNo
     * (local csn), use the latest one. GLT won't advance local csn if
     * nextCommitSeqNo > global csn, because GLT hasn't been able to actively
     * advance the global csn.
     *
     * @param xid: xid of the transaction to be committed
     */
    void (*getCommitCSNInTwophase)(TransactionId xid);

    /*
     * check a sql if is a glt command, shardingsphere send the global csn to
     * dns by a dql in distribute transaction for  visibility judgment and
     * transaction commit. If it's a glt command, dn will extract the global
     * csn in sql and save it.
     *
     * @param query_string: a query string
     */
    bool (*checkIsGLTCommand)(const char *query_string);

    /*
     * get csn for snapshot csn. GLT will compare global csn and nextCommitSeqNo
     * (local csn), use the latest one.
     * this interface is reserved for future optimization.
     *
     * @param csn: the csn for judging visibility
     */
    CommitSeqNo (*getSnapshotCSN)();

    /*
     * get xmin by LRUCache or globalXmin
     *
     * @param csn: the snapshot csn
     * @param xmin: the snapshot xmin
     */
    TransactionId (*getSnapshotXmin)(CommitSeqNo csn, TransactionId xmin);

} GLTMethods;

extern bool enable_glt;
extern GLTMethods *gltMethods;

/* methods if enable glt*/
extern CommitSeqNo gltGetCommitCSNBeforeCommitInProgress(CommitSeqNo csn, TransactionId xid);
extern void gltGetCommitCSNInXact(TransactionId xid);
extern void gltGetCommitCSNInTwophase(TransactionId xid);
extern bool gltCheckCommand(const char *query_string);
extern CommitSeqNo gltGetSnapshotCSN();
extern TransactionId gltGetSnapshotXmin(CommitSeqNo csn, TransactionId xmin);

/* methods if not enable glt*/
extern CommitSeqNo gltOffGetCommitCSNBeforeCommitInProgress(CommitSeqNo csn, TransactionId xid);
extern void gltOffGetCommitCSNInXact(TransactionId xid);
extern void gltOffGetCommitCSNInTwophase(TransactionId xid);
extern bool gltOffCheckCommand(const char *query_string);
extern CommitSeqNo gltOffGetSnapshotCSN();
extern TransactionId gltOffGetSnapshotXmin(CommitSeqNo csn, TransactionId xmin);


/* init gltMethods according to 'enable_glt' in postgres.conf */
extern void gltInitMethod(bool flag);
#endif