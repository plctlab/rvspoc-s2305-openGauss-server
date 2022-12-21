#include "access/glt.h"
#include "access/slru.h"
#include "storage/procarray.h"

/* the max lenght of sql for glt*/
const int glt_max_token_num = 5;
const int glt_command_length = 40;
const TransactionId glt_default_xmin = 2;
const CommitSeqNo invalid_csn = -1;
const TransactionId GLT_ERROR_XID = -1;
bool enable_glt = false;
GLTMethods* gltMethods = NULL;

void gltInitMethod(bool flag);

CommitSeqNo gltGetCommitCSNBeforeCommitInProgress(CommitSeqNo csn, TransactionId xid);
void gltSetLRUCacheXid(CommitSeqNo csn, TransactionId xid);
void gltGetCommitCSNInXact(TransactionId xid);
void gltGetCommitCSNInTwophase(TransactionId xid);
CommitSeqNo separatingGLTCommand(const char* query_string, char* (&token)[10000]);
CommitSeqNo string2UnsignLong(const char* str);
bool gltCheckCommand(const char* query_string);
CommitSeqNo gltGetSnapshotCSN();
TransactionId gltGetSnapshotXmin(CommitSeqNo csn, TransactionId xmin);

CommitSeqNo gltOffGetCommitCSNBeforeCommitInProgress(CommitSeqNo csn, TransactionId xid);
void gltOffGetCommitCSNInXact(TransactionId xid);
void gltOffGetCommitCSNInTwophase(TransactionId xid);
bool gltOffCheckCommand(const char* query_string);
CommitSeqNo gltOffGetSnapshotCSN();
TransactionId gltOffGetSnapshotXmin(CommitSeqNo csn, TransactionId xmin);

void gltInitMethod(bool flag)
{
    if (flag) {
        enable_glt = true;
        gltMethods = (GLTMethods*) malloc(sizeof(GLTMethods));
        gltMethods->getCommitCSNBeforeCommitInProgress = gltGetCommitCSNBeforeCommitInProgress;
        gltMethods->getCommitCSNInTwophase = gltGetCommitCSNInTwophase;
        gltMethods->getCommitCSNInXact = gltGetCommitCSNInXact;
        gltMethods->checkIsGLTCommand = gltCheckCommand;
        gltMethods->getSnapshotCSN = gltGetSnapshotCSN;
        gltMethods->getSnapshotXmin = gltGetSnapshotXmin;
    } else {
        enable_glt = false;
        gltMethods = (GLTMethods*) malloc(sizeof(GLTMethods));
        gltMethods->getCommitCSNBeforeCommitInProgress = gltOffGetCommitCSNBeforeCommitInProgress;
        gltMethods->getCommitCSNInTwophase = gltOffGetCommitCSNInTwophase;
        gltMethods->getCommitCSNInXact = gltOffGetCommitCSNInXact;
        gltMethods->checkIsGLTCommand = gltOffCheckCommand;
        gltMethods->getSnapshotCSN = gltOffGetSnapshotCSN;
        gltMethods->getSnapshotXmin = gltOffGetSnapshotXmin;
    }
}

CommitSeqNo gltGetCommitCSNBeforeCommitInProgress(CommitSeqNo csn, TransactionId xid) 
{
    LWLockAcquire(GLTLRULock, LW_SHARED);
    ereport(DEBUG1,
        (errmsg("LWLockAcquire(GLTLRULock, LW_EXCLUSIVE)")));
    CommitSeqNo latestCSN;

    if (t_thrd.proc->gltCommitCSN != 0) {
        latestCSN = t_thrd.proc->gltCommitCSN;
    } else {
        latestCSN = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo - 1;
    }
    latestCSN = latestCSN > csn ? latestCSN : csn;
    LWLockRelease(GLTLRULock);
    ereport(DEBUG1,
         (errmsg("LWLockRelease(GLTLRULock)")));
    return latestCSN;
}

void getCommitCSN(TransactionId xid) {
    LWLockAcquire(GLTLRULock, LW_EXCLUSIVE);
    ereport(DEBUG1,
        (errmsg("LWLockAcquire(GLTLRULock, LW_EXCLUSIVE)")));
    if (t_thrd.proc->gltCommitCSN != 0) {
        t_thrd.proc->commitCSN = t_thrd.proc->gltCommitCSN;
    } else {
        t_thrd.proc->commitCSN = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo - 1;
    }
    if (t_thrd.proc->gltCommitCSN >= t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo) {
        t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = t_thrd.proc->gltCommitCSN + 1;
    }
    t_thrd.proc->gltCommitCSN = 0;
    t_thrd.proc->gltSnapshotCSN = 0;

    LWLockRelease(GLTLRULock);
    ereport(DEBUG1,
         (errmsg("LWLockRelease(GLTLRULock)")));
}

void gltGetCommitCSNInXact(TransactionId xid)
{
    getCommitCSN(xid);
}

void gltGetCommitCSNInTwophase(TransactionId xid)
{
    getCommitCSN(xid);
}

CommitSeqNo separatingGLTCommand(const char* query_string, char token[][glt_command_length])
{
    char chBuffer[glt_command_length];
    char *pchDilem = " ";
    char *pchStrTmpIn = NULL;
    char *pchTmp = NULL;
    int num = 0;
    strncpy_s(chBuffer, sizeof(chBuffer), query_string, sizeof(chBuffer) - 1);
    pchTmp = chBuffer;

    while(NULL != ( pchTmp = strtok_r( pchTmp, pchDilem, &pchStrTmpIn) ) && num <= glt_command_length)
	{
		strncpy_s(token[num], sizeof(token[num]), pchTmp, sizeof(token[num]) - 1);
		pchTmp = NULL;
        num++;
	}
    return num;
}

/*
 * check a request is the glt command from shardingsphere
 * there are two kind of template "SELECT XXXX AS SETSNAPSHOTCSN" and "SELECT XXXXX AS SETSNAPSHOTCSN"
 * if true, store the csn in a private variable of thread
 * if false, do nothing
 */
bool gltCheckCommand(const char* query_string)
{
    const int glt_request_token_num = 4;
    if (strlen(query_string) == 0 || strlen(query_string) >= glt_command_length) {
        return false;
    }
    const char* set_snapshotcsn_request = "SETSNAPSHOTCSN";
    const char* set_commitcsn_request = "SETCOMMITCSN";
    const char* select_substring = "SELECT";
    const char* as_substring = "AS";
    CommitSeqNo num = 0;
    char token[glt_max_token_num][glt_command_length];
    memset(token, 0, sizeof(token));

    num = separatingGLTCommand(query_string, token);


    if (num != glt_request_token_num) {
        return false;
    }
    if (strncmp(token[0], select_substring, strlen(select_substring)) != 0 || strncmp(token[2], as_substring, strlen(as_substring)) != 0) {
        return false;
    }
    if (strncmp(token[3], set_snapshotcsn_request, strlen(set_snapshotcsn_request)) == 0) {
        CommitSeqNo csn = string2UnsignLong(token[1]);
        if (csn == invalid_csn) {
            return false;
        }
        t_thrd.proc->gltSnapshotCSN = csn;
    } else if (strncmp(token[3], set_commitcsn_request, strlen(set_commitcsn_request)) == 0) {
        CommitSeqNo csn = string2UnsignLong(token[1]);
        if (csn == invalid_csn) {
            return false;
        }
        t_thrd.proc->gltCommitCSN = csn;
    } else {
        return false;
    }
    return true;
}

/*
 * parse a string into uint64
 * if the string can't be parsed, return -1
 */
CommitSeqNo string2UnsignLong(const char* str) 
{
    CommitSeqNo csn = 0;

    if (str == NULL || strlen(str) == 0) {
        return invalid_csn;
    }

    for (uint i = 0; i < strlen(str); i++) {
        if (str[i] >= '0' && str[i] <= '9') {
            csn = csn * 10 + (str[i] - '0');
        } else {
            return invalid_csn;
        }
    }
    return csn;
}

CommitSeqNo gltGetSnapshotCSN()
{
    ereport(DEBUG1,
        (errmsg("gltGetSnapshotCSN: gltSnapshotCSN  %ld nextCommitSeqNo %ld",
            t_thrd.proc->gltSnapshotCSN,
            pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo) )));
    if (t_thrd.proc->gltSnapshotCSN != 0) {
        return t_thrd.proc->gltSnapshotCSN;
    } else {
        return pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
    }
}

TransactionId gltGetSnapshotXmin(CommitSeqNo csn, TransactionId xmin) 
{
    TransactionId result;
    if (t_thrd.proc->gltSnapshotCSN != 0 && enable_glt) {
        result = (TransactionId)glt_default_xmin;
    } else {
        result =  xmin;
    }
    ereport(DEBUG1,
        (errmsg("gltGetSnapshotXmin: csn %ld xmin %ld result %ld",
                csn, xmin, result)));
    return result;
}

CommitSeqNo gltOffGetCommitCSNBeforeCommitInProgress(CommitSeqNo csn, TransactionId xid)
{
    CommitSeqNo latestCSN = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    latestCSN = latestCSN > csn ? latestCSN : csn;
    return latestCSN;
}

void gltOffGetCommitCSNInXact(TransactionId xid)
{
    setCommitCsn(getLocalNextCSN());
}

void gltOffGetCommitCSNInTwophase(TransactionId xid)
{
    setCommitCsn(getLocalNextCSN());
}

/*
 * check a request is the glt command from shardingsphere
 * there are two kind of template "SELECT XXXX AS SETSNAPSHOTCSN" and "SELECT XXXXX AS SETSNAPSHOTCSN"
 * if true, store the csn in a private variable of thread
 * if false, do nothing
 */
bool gltOffCheckCommand(const char* query_string)
{
    return false;
}

CommitSeqNo gltOffGetSnapshotCSN()
{
    return pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
}

TransactionId gltOffGetSnapshotXmin(CommitSeqNo csn, TransactionId xmin) 
{
    return xmin;
}
