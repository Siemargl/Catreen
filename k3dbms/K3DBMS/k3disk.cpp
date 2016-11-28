/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Class k3diskgroup - file block operation (see also k3entity::scans)
 *
 * Modifications history:
 * 100423 Drop empty file if block void
 * 100507 - Refactoring, file-fullscan moved from entity to diskgroup
 * 100524 Check range by directory fixed
 */

#include "stdafx.h"



using namespace std;
#include "k3dbms.h"
#include "CCRC32.h"


#pragma warning(disable : 4996)

static CCRC32 crcTbl;

//================================ realisation ==================================

typedef map<wstring, BYTE*> emulatemap;
// emulatemap emulatefile;  // no emulation for fullscan

k3diskgroup::k3diskgroup(wstring path)
// .\DATA or \\SERVER\LONGPATH
{
	//memset(emulatefile, 255, sizeof emulatefile);
	LPTSTR part;
	wchar_t fdir[MAX_PATH];
    if(GetFullPathName(path.c_str(), MAX_PATH, fdir, &part) == 0)
	{
		throw k3except(L"k3diskgroup::k3diskgroup(). Can't get GetFullPathName()");
	}
	m_path = fdir;
}

void	k3diskgroup::reader(wstring mfile, k3bufferblock& bl, HANDLE hTrans)
{
    /// file struct long crc32, long len, databytes[len]
	_ASSERT(bl.m_state != k3bufferblock::dirty);

	HANDLE	hFile;
	DWORD	dwResult, dwSize;
	int		rc = 0;

	mfile = m_path + L"\\" + mfile;
	
	do {
		bl.m_used_sz = 0;
		bl.m_state = k3bufferblock::invalid;

		hFile = K3CreateFile(mfile.c_str(),               // file to open
				   GENERIC_READ,          // open for reading
				   FILE_SHARE_WRITE,       // share for reading
				   NULL,                  // default security
				   OPEN_EXISTING,         // must exist
				   FILE_ATTRIBUTE_NORMAL, // normal file
				   NULL,					// no attr. template
				  hTrans);

		if (hFile == INVALID_HANDLE_VALUE)
		{
			rc = GetLastError();
			if (rc != ERROR_FILE_NOT_FOUND && rc != ERROR_PATH_NOT_FOUND)
			{
				wchar_t	estr[MAX_PATH];
				wsprintf(estr, L"k3diskgroup::reader() Could not open file %s (error %d)", mfile.c_str(), rc);
				_ASSERT(0);
				throw k3except(estr);
			}
			bl.m_used_sz = 0;
			//rc = 1;
			break;
		}

	_RPTW2(_CRT_WARN, L"k3diskgroup::reader(%s) HNDL=%X\n", mfile.c_str(), hTrans);

		dwSize = GetFileSize(hFile, NULL);   // 32bit size max
		if (dwSize ==  INVALID_FILE_SIZE)
		{
	        _ASSERT(0);
			throw k3except(L"k3diskgroup::reader() File size error");
		}

		bl.resize(dwSize);

		if(!ReadFile (hFile, bl.m_buffer, dwSize, &dwResult, NULL))
		{
			wchar_t	estr[MAX_PATH];
			wsprintf(estr, L"k3diskgroup::reader() Could not read file %s (error %d)", mfile.c_str(), GetLastError());
	        _ASSERT(0);
			throw k3except(estr);
		}

		_ASSERT(dwSize == dwResult);

		if(!CloseHandle(hFile))
		{
			wchar_t	estr[MAX_PATH];
			wsprintf(estr, L"k3diskgroup::reader() Could not read file %s (error %d)", mfile.c_str(), GetLastError());
	        _ASSERT(0);
			throw k3except(estr);
		}

	    BYTE* ap = bl.m_buffer;
	    long crcread = *(long*)ap;
		bl.m_used_sz = dwSize - sizeof(long);
	    memmove(bl.m_buffer, (long*)ap + 1, bl.m_used_sz);
	    if (crcTbl.FullCRC(bl.m_buffer, bl.m_used_sz) != crcread)
	    {
	        _ASSERT(0);
	        throw k3except(L"k3diskgroup::reader() Block CRC failed on read");
	    }

		bl.m_state = k3bufferblock::valid;

	}while(0);

//	_RPTW1(_CRT_WARN, L"k3diskgroup::reader done(%s)\n", mfile.c_str());


	/* //emulation {{
	emulatemap::iterator it;
	it = emulatefile.find(m_file);

	if (it == emulatefile.end())
	{  // EOF
	    bl.m_used_sz = 0;
	} else
	{
	    BYTE* ap(it->second);
	    long crcread = *(long*)ap;
	    long blsize = *((long*)ap + 1);
	    if (blsize > bl.m_size)
	    {
		    delete bl.m_buffer;     bl.m_buffer = 0;
		    bl.m_buffer = new BYTE[blsize];
		    bl.m_size = blsize;
	    }
	    memcpy(bl.m_buffer , (long*)ap + 2, blsize);
		bl.m_used_sz = blsize;
	    if (crcTbl.FullCRC(bl.m_buffer, bl.m_used_sz) != crcread)
	    {
	        _ASSERT(0);
	        throw k3except(L"Block CRC failed on read");
	    }
	} //emulation }}
	*/
}


void	k3diskgroup::writer(wstring mfile, k3bufferblock& bl, HANDLE hTrans)  // thread may do sync dirty blocks (if free)
{
	HANDLE	hFile;
	DWORD	dwResult;
	int		rc = 0;

	_RPTW2(_CRT_WARN, L"k3diskgroup::writer(%s) HNDL=%X\n", mfile.c_str(), hTrans);

	mfile = m_path + L"\\" + mfile;

	do {
		LPTSTR part;
		wchar_t fdir[MAX_PATH];
#pragma message("TODO: GetFullPathName must be replaced for safety reason")

		if(GetFullPathName(mfile.c_str(), MAX_PATH, fdir, &part) == 0)
		{
			throw k3except(L"k3diskgroup::writer() Can't get GetFullPathName()");
		}
		*part = NULL;
		if(K3CreateDirectory(fdir, NULL, hTrans) == 0 && GetLastError()!= ERROR_ALREADY_EXISTS)
		{
			throw k3except(L"k3diskgroup::writer() Can't CreateDirectory()");
		}

        // drop empty blockfile
        if (bl.m_used_sz < sizeof(k3obj))
        {
			if (!K3DeleteFile(mfile.c_str(), hTrans) && GetLastError()!= ERROR_FILE_NOT_FOUND)
            {
                rc = GetLastError();
                wchar_t	estr[MAX_PATH];
                wsprintf(estr, L"k3diskgroup::writer() Could not delete file %s (error %d)", mfile.c_str(), rc);
                throw k3except(estr);
            };
            break;
        }

		hFile = K3CreateFile(mfile.c_str(),      // file to open
				   GENERIC_WRITE,          // open for writing
				   0,       // share for reading
//				   FILE_SHARE_READ,       // share for reading
				   NULL,                  // default security
				   CREATE_ALWAYS,         // overwrite
				   FILE_ATTRIBUTE_NORMAL, // normal file
				   NULL,					// no attr. template
				   hTrans);

		if (hFile == INVALID_HANDLE_VALUE)
		{
			rc = GetLastError();
			wchar_t	estr[MAX_PATH];
			wsprintf(estr, L"k3diskgroup::writer() Could not create file %s (error %d)", mfile.c_str(), rc);
			throw k3except(estr);
		}

		long crcwrt = crcTbl.FullCRC(bl.m_buffer, bl.m_used_sz);

		if(!WriteFile (hFile, &crcwrt, sizeof crcwrt, &dwResult, NULL))
		{
			wchar_t	estr[MAX_PATH];
			wsprintf(estr, L"k3diskgroup::writer() Could not write1 file %s (error %d)", mfile.c_str(), GetLastError());
			throw k3except(estr);
		}

		if(!WriteFile (hFile, bl.m_buffer, bl.m_used_sz, &dwResult, NULL))
		{
			wchar_t	estr[MAX_PATH];
			wsprintf(estr, L"k3diskgroup::writer() Could not write2 file %s (error %d)", mfile.c_str(), GetLastError());
			throw k3except(estr);
		}

		_ASSERT(bl.m_used_sz == dwResult);

		if(!CloseHandle(hFile))
		{
			wchar_t	estr[MAX_PATH];
			wsprintf(estr, L"k3diskgroup::writer() Could not close file %s (error %d)", mfile.c_str(), GetLastError());
			throw k3except(estr);
		}

	}while(0);

	_RPTW1(_CRT_WARN, L"k3diskgroup::writer done(%s)\n", mfile.c_str());

	/* //emulation {{
	emulatemap::iterator it;
	it = emulatefile.find(m_file);

	if (it == emulatefile.end())
	{  // new file
	    emulatefile.insert(emulatemap::value_type(m_file, new BYTE[bl.m_used_sz + 2 * sizeof(long)]));
	    it = emulatefile.find(m_file);
	} else
	{
		delete it->second;
		it->second = new BYTE[bl.m_used_sz + 2 * sizeof(long)];
	}

	BYTE* ap(it->second);

    long crcwrt = crcTbl.FullCRC(bl.m_buffer, bl.m_used_sz);
    long blsize = bl.m_used_sz;
    *((long*)ap + 0) = crcwrt;
    *((long*)ap + 1) = blsize;

    memcpy((long*)ap + 2, bl.m_buffer, blsize);
	//emulation }}
	*/
}


void	k3diskgroup::createEntity(const wstring &e3name, HANDLE hTrans)
// create directory, if not exists
{
	wstring	dirpath = m_path + L"\\" + e3name;

	if(K3CreateDirectory(dirpath.c_str(), NULL, hTrans) == 0 && GetLastError()!= ERROR_ALREADY_EXISTS)
	{
		throw k3except(L"k3diskgroup::createEntity(). Can't CreateDirectory()");
	}

}


struct dropBlockFunctorData
{
	dropBlockFunctorData(k3diskgroup* pme, HANDLE hT)
		: me(pme), hTrans(hT) {};
	k3diskgroup* me;
	HANDLE	hTrans;
};

bool	k3diskgroup::dropBlockFunctor(const wstring &ename, void* param)
{	
	dropBlockFunctorData* pdata = reinterpret_cast<dropBlockFunctorData*>(param);

	wstring	fullpath = pdata->me->m_path + L"\\" + ename;
	
    if (!K3DeleteFile(fullpath.c_str(), pdata->hTrans) && GetLastError()!= ERROR_FILE_NOT_FOUND)
    {
        int rc = GetLastError();
        wchar_t	estr[MAX_PATH];
        wsprintf(estr, L"k3diskgroup::dropBlockFunctor() Could not delete file %s (error %d)", fullpath.c_str(), rc);
        throw k3except(estr);
    };

	// remove conteinment dir
	LPTSTR part;
	wchar_t fdir[MAX_PATH];
#pragma message("TODO: GetFullPathName must be replaced for safety reason")
	if(GetFullPathName(fullpath.c_str(), MAX_PATH, fdir, &part) == 0)
	{
		throw k3except(L"k3diskgroup::dropBlockFunctor() Can't get GetFullPathName()");
	}
	*part = NULL;
	if(K3RemoveDirectory(fdir, pdata->hTrans) == 0) //&& GetLastError()!= ERROR_ALREADY_EXISTS)
	{
		DWORD res =  GetLastError();
		if (res != ERROR_DIR_NOT_EMPTY)
			throw k3except(L"k3diskgroup::dropBlockFunctor() Can't RemoveDirectory()");
	}

	return false;
}


void	k3diskgroup::dropEntity(const wstring &e3name, HANDLE hTrans)
{
	wstring	dirpath = m_path + L"\\" + e3name;

	dropBlockFunctorData data(this, hTrans);

	forEachBlockRangeHelper(dirpath, dropBlockFunctor, &data, L"", L"", hTrans);
}


bool	k3diskgroup::forEachBlockRange(const wstring &e3name, k3foreachblockfunc functor, void* param, const wstring &beginfname, const wstring &endfname, HANDLE hTrans)
{
	_RPTW3(_CRT_WARN, L"FULLSCAN k3diskgroup::forEachBlockRange = %s [%s, %s]\n", e3name.c_str(), beginfname.c_str(), endfname.c_str() );
    _ASSERT(beginfname == L"" || endfname == L"" || beginfname <= endfname);
//	if (beginfname > endfname)
//		throw k3except(L"k3diskgroup::forEachBlockRange. Invalid range");

	wstring	dirpath = m_path + L"\\" + e3name;
	wstring	beginpath, endpath;

	if (beginfname != L"")
		beginpath = dirpath + L"\\" + beginfname;
	if (endfname != L"")
		endpath = dirpath + L"\\" + endfname;
	return forEachBlockRangeHelper(dirpath, functor, param, beginpath, endpath, hTrans);
};


bool	k3diskgroup::forEachBlockRangeHelper(const wstring &fpath, k3foreachblockfunc functor, void* param, 
													const wstring &beginfile, const wstring &endfile, HANDLE hTrans)
// return true if breaked
{
//    LPTSTR part;
    wchar_t tmp[MAX_PATH+1]; // temporary array
	int		tmp_len;
//    wchar_t name[MAX_PATH];
    wchar_t next[MAX_PATH+1];
	bool	rc = false;

	HANDLE hSearch = NULL;
    WIN32_FIND_DATA wfd;
    memset(&wfd, 0, sizeof(WIN32_FIND_DATA));

    //search in embedded folders for first
    if(1)
    {
		/*
        if(GetFullPathName(fpath.c_str(), MAX_PATH, tmp, &part) == 0)
		{
			throw k3except(L"k3diskgroup::forEachBlockRange. Can't get GetFullPathName()");
		}
		*/
		wcscpy(tmp, fpath.c_str());


		tmp_len = wcslen(tmp);
		wcscat(tmp, L"\\*");

        //if folder exists, down to it
        wfd.dwFileAttributes = FILE_ATTRIBUTE_DIRECTORY;
        if ((hSearch = K3FindFirstFileEx(tmp, FindExInfoStandard, &wfd,
			FindExSearchLimitToDirectories, NULL, 0, hTrans)) == INVALID_HANDLE_VALUE)
		{
			if (GetLastError() != ERROR_FILE_NOT_FOUND)
				throw k3except(L"k3diskgroup::forEachBlockRange. Can't get FindFirstFile()");
		} else
		do
        {
            //if current folder is a service folders? which no need to check
            if (!wcsncmp(wfd.cFileName, L".", 1) || !wcsncmp(wfd.cFileName, L"..", 2)) continue;

            if (wfd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) //if it's a folder
            {
                // making a new path
				wcscpy(next, tmp);
                wcscpy(next + tmp_len+1, wfd.cFileName);

#pragma message("TODO: check range by directory not working =)")
                //check range by directory. 
                if (endfile != L"" && wcsncmp(endfile.c_str(), next, wcslen(next)) < 0) break; 
                if (beginfile == L"" || wcsncmp(beginfile.c_str(), next, wcslen(next)) <= 0)
                {
                    //recourse down
                    rc = forEachBlockRangeHelper(next, functor, param, beginfile, endfile, hTrans);
                    if (rc) break;	// breaked inside subdir
                }
            } else  // (****) traversing all files in NTFS, FindExSearchLimitToDirectories - ignored =(
            {
                wcscpy(next, tmp);
                wcscpy(next + tmp_len+1, wfd.cFileName);

                //check range by block name
                if (endfile != L"" && endfile < next) break;
                if (beginfile == L"" || beginfile <= next)
                {
                    // we've found next file
					// remove diskgroup path from it
					int pathlen = m_path.length();
                    rc = functor(next + pathlen + 1, param);
                    if(rc) break;
                }
            }

        }
        while (FindNextFile(hSearch, &wfd)); //finding next file in folder
        //---------------------------------------------------------------------
        FindClose (hSearch); // closing a search descriptor
    }
/* // commented because  (****)
	if (!rc)
	{
        if ((hSearch = K3FindFirstFileEx(tmp, FindExInfoStandard, &wfd,
			FindExSearchNameMatch, NULL, 0, m_trans->m_transact )) == INVALID_HANDLE_VALUE)
		{
			if (GetLastError() != ERROR_FILE_NOT_FOUND)
				throw k3except(L"k3diskgroup::forEachBlockRange. Can't get FindFirstFile():2");
		} else
		do
		if (!(wfd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) // is it's a file
		{
			wcscpy_s(next, MAX_PATH, tmp);
            wcscpy_s(next + tmp_len+1, MAX_PATH-tmp_len-1, wfd.cFileName);

			// we've found next
			k3bufferblock	bl(*this, -1);  // omit key1st from filename, set -1
			m_disk->reader(next, bl, getTransactHdnl());
			m_disk->m_statistics += k3stat(1, 0, 1, 0);

			rc = bl.forEach(k3func, param, m_factory);
			if(rc) break;
			//adding its to a result list
			//cout << file << endl;
		//******************************************
		//file_out << file << std::endl;
		//******************************************
		}
		while (FindNextFile(hSearch, &wfd)); // next file in folder
		FindClose (hSearch); // close search descriptor
	}
*/
	return rc;
}
