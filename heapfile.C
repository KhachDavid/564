#include "heapfile.h"
#include "error.h"
// include string library
#include <string.h>

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.

        // create the file
        status = db.createFile(fileName);
        if (status != OK)
        {
            cout << "Error creating file " << fileName << endl;
            return status;
        }

        // open the file
        status = db.openFile(fileName, file);
        if (status != OK)
        {
            cout << "Error opening file " << fileName << endl;
            return status;
        }

        // allocate an empty page by invoking bm->allocPage() appropriately
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK)
        {
            cout << "Error allocating header page for file " << fileName << endl;
            return status;
        }

        // cast the new page into a header page
        // allocPage() will return a pointer to an empty page in the buffer pool along with the page number of the page

        // Take the Page* pointer returned from allocPage() and cast it to a FileHdrPage*.
        hdrPage = (FileHdrPage *)newPage;

        int fileSize = fileName.size();

        // copy the file name into the header page
        // The file name is a string. You can use the strncpy() function to copy the file name into the header page.
        strncpy(hdrPage->fileName, fileName.c_str(), fileSize);
        // add /0 to the end of the file name to avoid garbage
        hdrPage->fileName[fileSize] = '\0';

        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK)
        {
            return status;
        }

        // set up the header page
        newPage->init(newPageNo);
        newPage->setNextPage(-1);

        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // unpin the header page
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);

        // close the file
        bufMgr->flushFile(file);
        db.closeFile(file);

        return OK;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile(fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
{
    Status status;
    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        File *file = filePtr;

        int pageNo = -1;
        status = file->getFirstPage(pageNo);
        if (status != OK)
        {
            cerr << "getFirstPage failed\n";
            returnStatus = status;
        }

        Page *page_ptr;
        status = bufMgr->readPage(file, pageNo, page_ptr);
        if (status != OK)
        {
            cerr << "readPage failed\n";
            returnStatus = status;
        }

        headerPage = reinterpret_cast<FileHdrPage *>(page_ptr);
        headerPageNo = pageNo;
        hdrDirtyFlag = false;

        int firstPageNo = headerPage->firstPage;
        status = bufMgr->readPage(file, firstPageNo, page_ptr);
        if (status != OK)
        {
            cerr << "readPage failed\n";
            returnStatus = status;
            return;
        }

        curPage = page_ptr;
        curPageNo = firstPageNo;
        curDirtyFlag = false;

        curRec = NULLRID;
        returnStatus = OK;
        return;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK)
            cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    if (curPage && curPageNo == rid.pageNo)
    {
        curRec = rid;
        status = curPage->getRecord(curRec, rec);
    }
    else
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
        {
            return status;
        }

        curPageNo = rid.pageNo, curRec = rid, curDirtyFlag = false;

        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;

        status = curPage->getRecord(curRec, rec);
    }
    return status;
}

HeapFileScan::HeapFileScan(const string &name,
                           Status &status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_)
{
    if (!filter_)
    {
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        ((type_ != INTEGER && type_ != FLOAT && type_ != STRING) ||
         (type_ == INTEGER && length_ != sizeof(int)) || (type_ == FLOAT && length_ != sizeof(float)) ||
         (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE)))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false; // it will be clean
    }
    else
        curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID &outRid)
{
    Status status = OK;
    RID nextRid;
    RID tmpRid;
    int nextPageNo;
    Record rec;

    Status initialRecStatus = OK;
    Status recordStatus = OK;
    Status firstRecStatus = OK;
    Status nextRecStatus = OK;

    if (!curPage)
    {

        curPageNo = headerPage->firstPage, curDirtyFlag = false;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;

        initialRecStatus = curPage->firstRecord(tmpRid);
        if (status != OK)
            return status;

        curRec = tmpRid;
    }

    while (initialRecStatus == OK || recordStatus == OK)
    {
        if (firstRecStatus == OK)
        {
            nextRecStatus = curPage->nextRecord(curRec, nextRid);
            if (nextRecStatus == OK)
                curRec = nextRid;
        }

        if (!(nextRecStatus == OK))
        {
            status = curPage->getNextPage(nextPageNo);
            if (status != OK)
            {
                return status;
            }
            if (nextPageNo == -1)
            {
                return FILEEOF;
            }

            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
            {
                return status;
            }

            curPageNo = nextPageNo, curDirtyFlag = false;

            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
                return status;

            firstRecStatus = curPage->firstRecord(curRec);
            if (!(firstRecStatus == OK))
            {
                continue;
            }
        }

        recordStatus = curPage->getRecord(curRec, rec);
        if (recordStatus != OK)
            return recordStatus;

        if (matchRec(rec))
        {
            outRid = curRec;
            return OK;
        }
    }

    return FILEEOF;
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const
{
    // no filtering requested
    if (!filter)
        return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
    switch (type)
    {

    case INTEGER:
        int iattr, ifltr; // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr; // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch (op)
    {
    case LT:
        return diff < 0.0;
        break;
    case LTE:
        return diff <= 0.0;
        break;
    case EQ:
        return diff == 0.0;
        break;
    case GTE:
        return diff >= 0.0;
        break;
    case GT:
        return diff > 0.0;
        break;
    case NE:
        return diff != 0.0;
        break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string &name,
                               Status &status) : HeapFile(name, status)
{
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    Page *newPage;
    int newPageNo;
    Status status;
    RID rid;

    if ((int)rec.length > PAGESIZE - DPFIXED)
    {
        return INVALIDRECLEN;
    }

    if (!curPage || curPageNo != headerPage->lastPage)
    {

        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
        {
            return status;
        }

        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;

        // set the dirty flag to false since we are reading the page
        curDirtyFlag = false;

        // check if the page is full
        status = curPage->firstRecord(curRec);
        if (status != OK)
            return status;
    }

    status = curPage->insertRecord(rec, rid);
    if (status == OK)
    {
        headerPage->recCnt++, curRec = rid, curDirtyFlag = true;
        outRid = rid;
        return OK;
    }

    // if the page is full, allocate a new page and insert the record
    else if (status == NOSPACE)
    {
        // allocate a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK)
        {
            return status;
        }

        newPage->init(newPageNo);
        headerPage->lastPage = newPageNo, headerPage->pageCnt++;

        curPage->setNextPage(newPageNo);

        // set the dirty flag to true since we are writing the page
        curDirtyFlag = true;

        // unpin the current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
        {
            return status;
        }

        // set the current page to the new page
        // it would be the last page in the file
        // because we just allocated it
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;

        status = curPage->insertRecord(rec, rid);
        if (status != OK)
            return status;

        // add the record to the current page
        headerPage->recCnt++;
        curRec = rid;

        // set the dirty flag to true since we are writing the page
        curDirtyFlag = true;

        // set it to -1 because we are not scanning the file
        curPage->setNextPage(-1);

        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
            return status;

        outRid = rid;
        return OK;
    }
    else
    {
        return status;
    }
}