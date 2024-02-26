/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * ss_reform_common.cpp
 *  common methods for crash recovery, switchover and failover.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_init.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"
#include "postmaster/postmaster.h"
#include "storage/smgr/fd.h"
#include "storage/dss/fio_dss.h"
#include "ddes/dms/ss_dms.h"
#include "ddes/dms/ss_common_attr.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_reform_common.h"
#include "storage/file/fio_device.h"
#include "storage/smgr/segment_internal.h"
#include "replication/walreceiver.h"
#include "replication/ss_cluster_replication.h"

/*
 * Add xlog reader private structure for page read.
 */
typedef struct XLogPageReadPrivate {
    int emode;
    bool fetching_ckpt; /* are we fetching a checkpoint record? */
    bool randAccess;
} XLogPageReadPrivate;

std::vector<int> SSGetAllStableNodeId()
{
    std::vector<int> posList;
    int pos = 0;
    uint64 stableInstId = g_instance.dms_cxt.SSReformerControl.list_stable;
    while (stableInstId) {
        uint64 res = stableInstId & 0x01;
        if (res) {
            posList.emplace_back(pos);
        }
        pos++;
        stableInstId = stableInstId >> 1;
    }

    return posList;
}

int SSXLogFileOpenAnyTLI(XLogSegNo segno, int emode, uint32 sources, char* xlog_path)
{
    char path[MAXPGPATH];
    ListCell *cell = NULL;
    int fd = -1;
    errno_t errorno = EOK;
    
    foreach (cell, t_thrd.xlog_cxt.expectedTLIs) {
        TimeLineID tli = (TimeLineID)lfirst_int(cell);
        if (tli < t_thrd.xlog_cxt.curFileTLI) {
            break; /* don't bother looking at too-old TLIs */
        }

        errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path, tli,
                             (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");
        t_thrd.xlog_cxt.restoredFromArchive = false;

        fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);

        if (fd >= 0) {
            /* Success! */
            t_thrd.xlog_cxt.curFileTLI = tli;

            /* Track source of data in assorted state variables */
            t_thrd.xlog_cxt.readSource = sources;
            t_thrd.xlog_cxt.XLogReceiptSource = (int)sources;

            /* In FROM_STREAM case, caller tracks receipt time, not me */
            if (sources != XLOG_FROM_STREAM) {
                t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();
            }

            return fd;
        }

        /* 
        * When SS_REPLICATION_DORADO_CLUSTER enabled, current xlog dictionary may be not the correct dictionary,
        * because all xlog dictionaries are in the same LUN, we need loop over other dictionaries.
        */
        if (fd < 0 && SS_REPLICATION_DORADO_CLUSTER) {
            return -1;
        }

        if (!FILE_POSSIBLY_DELETED(errno)) { 
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                              XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
        }
    }

    /* Couldn't find it.  For simplicity, complain about front timeline */
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path,
                         t_thrd.xlog_cxt.recoveryTargetTLI, (uint32)((segno) / XLogSegmentsPerXLogId),
                         (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    errno = ENOENT;
    ereport(emode, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                      XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno)))); 

    return -1;
}

int SSReadXlogInternal(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, XLogRecPtr targetRecPtr, char *buf,
    int readLen)
{
    uint32 preReadOff;
    XLogRecPtr xlogFlushPtrForPerRead = xlogreader->xlogFlushPtrForPerRead;
    bool isReadFile = true;

    Assert(readLen > 0);
    Assert(readLen <= XLogPreReadSize);

    do {
        /* 
         * That source is XLOG_FROM_STREAM indicate that walreceiver receive xlog and walrecwriter have wrriten xlog
         * into pg_xlog segment file in dss. There exists a condition which preReadBuf possibly is zero for some xlog
         * record just writing into pg_xlog file when source is XLOG_FROM_STREAM and dms and dss are enabled. So we
         * need to reread xlog from dss to preReadBuf.
         */
        if (SS_REPLICATION_MAIN_STANBY_NODE) {
            volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
            if (XLByteInPreReadBuf(targetPagePtr, xlogreader->preReadStartPtr) && 
               ((targetRecPtr < xlogFlushPtrForPerRead && t_thrd.xlog_cxt.readSource == XLOG_FROM_STREAM) || 
               (!xlogctl->IsRecoveryDone) || (t_thrd.xlog_cxt.readSource != XLOG_FROM_STREAM))) {
                   isReadFile = false;
               }
        }

        if ((XLByteInPreReadBuf(targetPagePtr, xlogreader->preReadStartPtr) &&
             !SS_REPLICATION_MAIN_STANBY_NODE) || (!isReadFile)) {
            preReadOff = targetPagePtr % XLogPreReadSize;
            int err = memcpy_s(buf, readLen, xlogreader->preReadBuf + preReadOff, readLen);
            securec_check(err, "\0", "\0");
            break;
        } else {
            if (SS_REPLICATION_MAIN_STANBY_NODE) {
                xlogreader->xlogFlushPtrForPerRead = GetWalRcvWriteRecPtr(NULL);
                xlogFlushPtrForPerRead = xlogreader->xlogFlushPtrForPerRead;
            }
            // pre-reading for dss
            uint32 targetPageOff = targetPagePtr % XLogSegSize;
            preReadOff = targetPageOff - targetPageOff % XLogPreReadSize;
            ssize_t actualBytes = pread(t_thrd.xlog_cxt.readFile, xlogreader->preReadBuf, XLogPreReadSize, preReadOff);
            if (actualBytes != XLogPreReadSize) {
                return false;
            }
            xlogreader->preReadStartPtr = targetPagePtr + preReadOff - targetPageOff;
        }
    } while (true);

    return readLen;
}

XLogReaderState *SSXLogReaderAllocate(XLogPageReadCB pagereadfunc, void *private_data, Size alignedSize)
{
    XLogReaderState *state = XLogReaderAllocate(pagereadfunc, private_data, alignedSize);
    if (state != NULL) {
        state->preReadStartPtr = InvalidXlogPreReadStartPtr;
        state->preReadBufOrigin = (char *)palloc_extended(XLogPreReadSize + alignedSize,
            MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
        if (state->preReadBufOrigin == NULL) {
            pfree(state->errormsg_buf);
            state->errormsg_buf = NULL;
            pfree(state->readBufOrigin);
            state->readBufOrigin = NULL;
            state->readBuf = NULL;
            pfree(state->readRecordBuf);
            state->readRecordBuf = NULL;
            pfree(state);
            state = NULL;
            return NULL;
        }

        if (alignedSize == 0) {
            state->preReadBuf = state->preReadBufOrigin;
        } else {
            state->preReadBuf = (char *)TYPEALIGN(alignedSize, state->preReadBufOrigin);
        }

        state->xlogFlushPtrForPerRead = InvalidXLogRecPtr;
    }

    return state;
}

void SSGetRecoveryXlogPath()
{
    errno_t rc = EOK;
    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;

    rc = snprintf_s(g_instance.dms_cxt.SSRecoveryInfo.recovery_xlog_dir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d",
        dssdir, g_instance.dms_cxt.SSRecoveryInfo.recovery_inst_id);
    securec_check_ss(rc, "", "");
}

void SSDoradoGetInstidList()
{
    for (int i = 0; i < DMS_MAX_INSTANCE; i++) {
        g_instance.dms_cxt.SSRecoveryInfo.instid_list[i] = -1;
    }
    struct dirent *entry;
    errno_t rc = EOK;
    DIR* dssdir = opendir(g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name);
    if (dssdir == NULL) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("Error opening dssdir %s", 
                        g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name)));                                                  
    }

    uint8_t len = strlen("pg_xlog");
    uint8_t index = 0;
    while ((entry = readdir(dssdir)) != NULL) {
        if (strncmp(entry->d_name, "pg_xlog", len) == 0) {
            if (strlen(entry->d_name) > len) {
                rc = memmove_s(entry->d_name, MAX_PATH, entry->d_name + len, strlen(entry->d_name) - len + 1);
                securec_check_c(rc, "\0", "\0");
                g_instance.dms_cxt.SSRecoveryInfo.instid_list[index++] = atoi(entry->d_name);
            }
        } else {
            continue;
        }
    }
    closedir(dssdir);
}

void SSUpdateReformerCtrl()
{
    int fd = -1;
    int len;
    errno_t err = EOK;
    char *fname[2];

    len = sizeof(ss_reformer_ctrl_t);
    int write_size = (int)BUFFERALIGN(len);
    char buffer[write_size] __attribute__((__aligned__(ALIGNOF_BUFFER))) = { 0 };

    err = memcpy_s(&buffer, write_size, &g_instance.dms_cxt.SSReformerControl, len);
    securec_check(err, "\0", "\0");

    INIT_CRC32C(((ss_reformer_ctrl_t *)buffer)->crc);
    COMP_CRC32C(((ss_reformer_ctrl_t *)buffer)->crc, (char *)buffer, offsetof(ss_reformer_ctrl_t, crc));
    FIN_CRC32C(((ss_reformer_ctrl_t *)buffer)->crc);

    fname[0] = XLOG_CONTROL_FILE_BAK;
    fname[1] = XLOG_CONTROL_FILE;

    for (int i = 0; i < BAK_CTRL_FILE_NUM; i++) {
        if (i == 0) {
            fd = BasicOpenFile(fname[i], O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        } else {
            fd = BasicOpenFile(fname[i], O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        }

        if (fd < 0) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname[i])));
        }

        SSWriteInstanceControlFile(fd, buffer, REFORM_CTRL_PAGE, write_size);
        if (close(fd)) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }
    }
}

void SSReadControlFile(int id, bool updateDmsCtx)
{
    pg_crc32c crc;
    errno_t rc = EOK;
    int fd = -1;
    char *fname = NULL;
    bool retry = false;
    int read_size = 0;
    int len = 0;
    fname = XLOG_CONTROL_FILE;
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

loop:
    fd = BasicOpenFile(fname, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        LWLockRelease(ControlFileLock);
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname)));
    }

    off_t seekpos = (off_t)BLCKSZ * id;

    if (id == REFORM_CTRL_PAGE) {
        len = sizeof(ss_reformer_ctrl_t);
    } else {
        len = sizeof(ControlFileData);
    }

    read_size = (int)BUFFERALIGN(len);
    char buffer[read_size] __attribute__((__aligned__(ALIGNOF_BUFFER)));
    if (pread(fd, buffer, read_size, seekpos) != read_size) {
        LWLockRelease(ControlFileLock);
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not read from control file: %m")));
    }

    if (id == REFORM_CTRL_PAGE) {
        rc = memcpy_s(&g_instance.dms_cxt.SSReformerControl, len, buffer, len);
        securec_check(rc, "", "");
        if (close(fd) < 0) {
            LWLockRelease(ControlFileLock);
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)&g_instance.dms_cxt.SSReformerControl, offsetof(ss_reformer_ctrl_t, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, g_instance.dms_cxt.SSReformerControl.crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                LWLockRelease(ControlFileLock);
                ereport(FATAL, (errmsg("incorrect checksum in control file")));
            }
        }
        g_instance.dms_cxt.SSRecoveryInfo.cluster_ondemand_status= g_instance.dms_cxt.SSReformerControl.clusterStatus;
    } else {
        ControlFileData* controlFile = NULL;
        ControlFileData tempControlFile;
        if (updateDmsCtx) {
            controlFile = &tempControlFile;
        } else {
            controlFile = t_thrd.shemem_ptr_cxt.ControlFile;
        }

        rc = memcpy_s(controlFile, (size_t)len, buffer, (size_t)len);
        securec_check(rc, "", "");
        if (close(fd) < 0) {
            LWLockRelease(ControlFileLock);
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)controlFile, offsetof(ControlFileData, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, controlFile->crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                LWLockRelease(ControlFileLock);
                ereport(FATAL, (errmsg("incorrect checksum in control file")));
            }
        }

        if (XLByteLE(g_instance.dms_cxt.ckptRedo, controlFile->checkPointCopy.redo)) {
            g_instance.dms_cxt.ckptRedo = controlFile->checkPointCopy.redo;
        }
    }
    LWLockRelease(ControlFileLock);
}

void SSClearSegCache()
{
    (void)LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
    HeapMemResetHash(t_thrd.storage_cxt.SegSpcCache, "Shared Seg Spc hash by request");
    LWLockRelease(ShmemIndexLock);
}

void SSStandbySetLibpqswConninfo()
{
    if (strlen(g_instance.dms_cxt.dmsInstAddr[g_instance.dms_cxt.SSReformerControl.primaryInstId]) == 0) {
        ereport(WARNING, (errmsg("Failed to get ip of primary node!")));
        return;
    }

    int replIdx = -1;
    ReplConnInfo *replconninfo = NULL;

    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replconninfo == NULL) {
            continue;
        }

        if (strcmp(replconninfo->remotehost,
            g_instance.dms_cxt.dmsInstAddr[g_instance.dms_cxt.SSReformerControl.primaryInstId]) == 0) {
            replIdx = i;
            break;
        }
    }

    if (replIdx == -1) {
        ereport(WARNING, (errmsg("Failed to get replconninfo of primary node, check the replconninfo config!")));
        return;
    }

    replconninfo = t_thrd.postmaster_cxt.ReplConnArray[replIdx];
    errno_t rc = EOK;
    rc = snprintf_s(g_instance.dms_cxt.conninfo, MAXCONNINFO, MAXCONNINFO - 1,
        "host=%s port=%d localhost=%s localport=%d", replconninfo->remotehost, replconninfo->remoteport,
        replconninfo->localhost, replconninfo->localport);
    securec_check_ss(rc, "\0", "\0");
    g_instance.dms_cxt.conninfo[MAXCONNINFO - 1] = '\0';

    return;
}
