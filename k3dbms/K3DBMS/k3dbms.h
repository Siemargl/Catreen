#ifndef _K3DBMS_H
#define _K3DBMS_H

/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  All library headers
 *
 * Modifications history:
 * 100428 Add Dependent Entities && k3link
 * 100429 Changed interface from retval by value for complex types to byref
 * 100511 Added templates serializeXXX. This partially loses Digital Mars C++ compatibility
 */

#pragma pack(4)

// using TxF (Vista) features
#define ACID

#ifdef ACID
// transacted functions called when handle id transacted - support mix transacted and non-trans.operations
#	include <Ktmw32.h>
#	define K3FindFirstFileEx(p1, p2, p3, p4, p5, p6, p7) ((p7 != INVALID_HANDLE_VALUE)?(FindFirstFileTransacted(p1, p2, p3, p4, p5, p6, p7)):FindFirstFileEx(p1, p2, p3, p4, p5, p6))
#	define K3CreateFile(p1, p2, p3, p4, p5, p6, p7, p8) ((p8 != INVALID_HANDLE_VALUE)?CreateFileTransacted(p1, p2, p3, p4, p5, p6, p7, p8, 0, 0):CreateFile(p1, p2, p3, p4, p5, p6, p7))
#	define K3CreateDirectory(p1, p2, p3) ((p3 != INVALID_HANDLE_VALUE)?CreateDirectoryTransacted(NULL, p1, p2, p3):CreateDirectory(p1, p2))
#	define K3DeleteFile(p1, p2) ((p2 != INVALID_HANDLE_VALUE)?DeleteFileTransacted(p1, p2):DeleteFile(p1))
#	define K3RemoveDirectory(p1, p2) ((p2 != INVALID_HANDLE_VALUE)?RemoveDirectoryTransacted(p1, p2):RemoveDirectory(p1))

#else

#	define K3FindFirstFileEx(p1, p2, p3, p4, p5, p6, p7) FindFirstFileEx(p1, p2, p3, p4, p5, p6)
#	define K3CreateFile(p1, p2, p3, p4, p5, p6, p7, p8) CreateFile(p1, p2, p3, p4, p5, p6, p7)
#	define K3CreateDirectory(p1, p2, p3) CreateDirectory(p1, p2)
#	define K3DeleteFile(p1, p2) DeleteFile(p1)
#	define K3RemoveDirectory(p1, p2) RemoveDirectory(p1)

#endif

typedef GUID    UID;   // HRESULT CoCreateGuid(__out  GUID *pguid);
class	k3trans;
class	k3entity;
class   k3entityDepends;
class   k3entityIndex;
const	int	k3def_transaction_cache_size = 50000; /// Cache size for each k3entity in KBytes
const	int	k3def_block_size = 64 * 1024 - 10;  // you can save file in MFT if size <= 620 (but slower)

//void	GenerateUID(UID &uid);

struct K3KEY
/// hash used for all indexing. no uniquieness required
{
	unsigned __int64 key[2];
	bool	isNull;
	K3KEY(unsigned __int64 k) 
	{
		key[0] = k; 
		key[1] = 0; 
		isNull = false; 
	}
	K3KEY(const K3KEY &k) { key[0] = k.key[0]; key[1] = k.key[1]; isNull = false; }
	K3KEY(const wstring &s);
    K3KEY(const SYSTEMTIME &s);
	K3KEY(const UID &uid);
	K3KEY() { isNull = true; }

	bool operator < (const K3KEY &b) const
	{
		return (key[1] < b.key[1]) || ((key[1] == b.key[1]) && (key[0] < b.key[0]));
	};
	bool operator == (const K3KEY &b) const
	{
		return (key[1] == b.key[1]) && (key[0] == b.key[0]);
	};
};

class k3except
/// Error exception class
{
public:
	int	m_error;	// GetLastError
	wstring	s_error; // error string
	k3except(int ec) {m_error = ec;}
	k3except(wstring e) {s_error = e; m_error = 0;}
	k3except(wstring e, int ec) {s_error = e; m_error = ec;}
	k3except(k3except &x) {m_error = x.m_error; s_error = x.s_error;}
};


class k3CriticalSection
// helper for auto-unlocking when throw
{
	LPCRITICAL_SECTION	hSection;
public:
	k3CriticalSection(LPCRITICAL_SECTION sect)
		: hSection(sect)
	{
		_ASSERT(hSection);
		if (hSection)
			EnterCriticalSection(hSection);
	};
	~k3CriticalSection()
	{
		if (hSection)
			LeaveCriticalSection(hSection);
	};
};


struct k3stat
/// Disk statistics for measuring performance
{
	k3stat();
	k3stat(int lr, int lwr, int phr, int phwr)
		: logical_reads(lr), block_changes(lwr), physical_reads(phr), physical_writes(phwr){};
	const k3stat   &operator +=(const k3stat&);

	string print();
    int	    logical_reads;
	int     block_changes;
	int	    physical_reads;
	int     physical_writes;
	//int	transactions;

	// Parses, Sorts
};

struct k3objheader
/// All database objects must begin with this (binary!)
{
    k3objheader(): m_prikey(-1) {};
    int		m_size;  // datasize, including this header
	UID		m_uid;   // uniquie identifier
	int 	m_version; // version (inc when saves)
	int 	m_scheme; // scheme version
	K3KEY	m_prikey;  // defines order in entity. Change after object creation is prohibited - may result in doubles!
};

class	k3obj
/// base class for storing object data in entities
{
	friend class k3bufferblock;
	friend class k3entity_cache;
	friend class k3entity;
protected:
	//BYTE*   m_data;
	bool    serializeOn;  // flag "emulate Serialization" for helpers
    // serialize helpers
    template <class Type>
    void serializeValue(BYTE* bin, int maxsz, Type m_pod, int &newlen);

#ifndef __DMC__
	template <>
	void serializeValue <wstring> (BYTE* bin, int maxsz, wstring m_str, int &newlen);

#endif
//    void serialize_Str(BYTE* bin, int maxsz, wstring m_str, int &newlen);

    template <class Type>
    void serializeArray(BYTE* bin, int maxsz, vector<Type> m_vec, int &newlen);

    template <class KeyType, class DataType>
    void serializeMap(BYTE* bin, int maxsz, map<KeyType, DataType> m_map, int &newlen);

    template <class Type>
    void deserializeValue(BYTE* bin, int maxsz, Type &m_pod, int &newlen);
#ifndef __DMC__
	template <>
	void deserializeValue <wstring> (BYTE* bin, int maxsz, wstring &m_str, int &newlen);
#endif

    template <class Type>
    void deserializeArray(BYTE* bin, int maxsz, vector<Type> &m_vec, int &newlen);

    template <class KeyType, class DataType>
    void deserializeMap(BYTE* bin, int maxsz, map<KeyType, DataType> &m_map, int &newlen);

public:
	k3objheader d;
	k3obj();
	//k3obj(const k3obj&);
	virtual ~k3obj();
	virtual bool    operator < (const k3obj&) const;  // compare by primary key
	virtual	k3obj&  operator = (const k3obj&);
	virtual	        operator UID();

	virtual wstring getPrimaryKeyName() const { return L"UID"; }
	virtual	void   getKey(const wstring &keyname, K3KEY &got) const { got = d.m_prikey; }
    // index ised by this name. recognized: UID, PRIMARY

	virtual k3obj*  create() const = 0;	// class factory (new())

	virtual	void	deserialize(BYTE*, int sz); // call from reader (disk->obj)
	virtual	void	serialize(BYTE*, int &sz);  // call from writer (obj->disk). MUST write header d 1st
	virtual	int	    getsize();  // if object has variable length fields, size may reclaimed with overhead
};

typedef	auto_ptr<k3obj>	k3objptr;               /// helper class - using near exception
typedef vector<k3objheader>	UIDvector;              /// return value for simple queries
typedef bool k3foreachfunc(k3obj*, void*);      /// return value for scan queries. true = break scan
typedef bool k3foreachblockfunc(const wstring &name, void*);      /// return value for scan queries. true = break scan
typedef multimap<K3KEY, int> k3blkcontens;   /// disk block header
//typedef list<k3obj*>	k3objlist;

class k3bufferblock
/// Disk && cache block. Contains one or more same type, but variable length k3obj
{
	friend class	k3entity_cache;
	friend class	k3entity;
	friend class	k3diskgroup;
	k3blkcontens    m_objoffsets;
	int		    m_lru;		// LRU counter
	K3KEY       m_key1st;		// 1st object
	BYTE*       m_buffer;

public:
	int		    m_size;		// blocksize (minimum file size);
	int		    m_used_sz;	// used by obj
	k3entity    &m_entity;
	enum	{invalid, valid, reserved, dirty} m_state;  // changed only by k3entity_cache

	int		find(const UID&);	 // internal func, -1 if none, linear search
	int		find(const k3objheader&);	 // internal func, -1 if none, multimap search
	void	read();	// disk operation
	void	readBack(const wstring &ename);	// disk operation
	void	write();
	bool	forEach(k3foreachfunc k3func, void* param, k3obj* factory); // return true if breaked
    bool	forEachKeyRange(const K3KEY &begin, const K3KEY &end, k3obj* factory, k3foreachfunc k3func, void* param);
    void    initKey1st();

	k3bufferblock(k3entity&, const K3KEY &key1);
	k3bufferblock(const k3bufferblock&);
	~k3bufferblock();

	const k3bufferblock&
            operator =(const k3bufferblock&);
	bool	load(const UID &, k3obj&);	// true if found
	bool	load(const K3KEY &key, UIDvector &ovec);	// true if found > 0. appends to vector
	bool	loadUni(const K3KEY &key, k3obj &obj);	// true if found 
	bool	load(const k3objheader &, k3obj&);	// true if found
	bool	save(k3obj&);  // ret true = addnew if need
	void	remove(const k3objheader&);
	void	remove(const UID &);
	void	remove(const K3KEY &);
	void	resize(int newsz);  // mem alloc
};


class	k3diskgroup
/// Proxy class for ONE disk spindle. Parallelizm && IO Queue must be here (later)
// think about caching open file handles (if file is big)
{
	friend class k3entity;
	friend class k3entity_cache;
	friend class k3bufferblock;

//	static 	list<k3diskgroup> m_all;
	wstring	m_path;
	int		m_parallelism;
	void	reader(wstring file, k3bufferblock&, HANDLE hTrans);  // read all data + check CRC32
	void	writer(wstring file, k3bufferblock&, HANDLE hTrans);  // thread may do sync dirty blocks (if free)
	bool	forEachBlockRangeHelper(const wstring &fpath, k3foreachblockfunc functor, void* param,
                            const wstring &beginfile, const wstring &endfile, HANDLE hTrans); //  need absolute path
	static	bool	dropBlockFunctor(const wstring &name, void*);
public:
	k3diskgroup(wstring path); // .\DATA or \\SERVER\LONGPATH
	void	createEntity(const wstring &e3name, HANDLE hTrans);
	void	dropEntity(const wstring &e3name, HANDLE hTrans);
	bool	forEachBlockRange(const wstring &e3name, k3foreachblockfunc functor, void* param,
                            const wstring &beginfile, const wstring &endfile, HANDLE hTrans); // return true if breaked
	k3stat	m_statistics;
};


typedef	map<K3KEY, k3bufferblock>	k3entitycache_map;      /// cache list for 1 entity
typedef map<wstring, k3entitycache_map> k3transcache_map;
    /// cache list for 1 transaction, str = entity name

typedef map<wstring, k3transcache_map> k3buffercache_map;
    /// global cache list, transaction list, str = transaction name

class	k3entity_cache
/// Cache worker class - the only class, working with buffer cache
{
	friend class k3trans;
	friend class k3entity;
	friend class k3entityIndex;
	friend class k3entityDepends;

	static k3buffercache_map	buffercache;
    static LPCRITICAL_SECTION	k3bufferCritSect;   /// Lock for buffer cache
    static int k3bufferRefcount;

	//static k3entitycache_map&	getcache(string ent);
	static void walkall_readmiss(const wstring &ent, const K3KEY &key1st);
	static void walkall_invalidate(const wstring &ent, const K3KEY &key1st);
    static bool forEachKeyRangeFunctor(const wstring &blname, void* param);
    static bool loadFunctor(k3obj* pobj, void* param);

	k3entitycache_map*  m_cache;
	k3entity	        &m_entity;
	k3stat		        m_statistics;

	k3entity_cache(k3entity&);
	~k3entity_cache();

	k3bufferblock*    loadBlock(const K3KEY &key1, bool caching); // return ref to cache
	void    updateBlock(k3bufferblock *bl, bool isnew = false); // update cached block & consistency
    bool	forEachKeyRange(const K3KEY &begin, const K3KEY &end, k3foreachfunc k3func, void* param);

	bool	load(const UID &, k3obj&);
	bool	load(const k3objheader &, k3obj&);
	bool	load(const K3KEY &key, UIDvector &ovec);
	bool	loadUni(const K3KEY &key, k3obj &obj);
	bool	save(k3obj&);   // true if addnew
	void	remove(const k3objheader &);
	void	remove(const UID&);
	void	remove(const K3KEY&);

	// bool	reader(UID, k3obj&);
	//bool	load(const K3KEY &key, UIDvector &ovec, const k3objheader *pObjh = NULL, k3obj *pObj = NULL);
        // if want one element - set pObjh && pObj
	//void	remove(const K3KEY &key, const k3objheader* objh = NULL);


	bool	cachescanReader(const UID&, k3obj&);	// true if found. cache-only operation
	bool	cachescanRemove(const UID&); // true if delete. cache-only operation

	//void	remove(UID);
	void	sweep();
	void    flush();
    void    discard();
public:
	static	void cleanStatics();  // call on exit app
};


class	k3entity
/// Main [base] class for storing objects
{
protected:
	friend class k3entity_cache;
	friend class k3bufferblock;
	friend class k3entityIndex;
	friend class k3entityDepends;

	wstring	    m_name; //, m_path;	// clercs, "c:\data\"
	wstring	    m_prikey;
	k3trans*    m_trans;		// NULL if nontransactional
	k3obj*	    m_factory;		// Class Factory for Entity Type
	int		    m_objsize;      // default size (plus delta-reserve)
	bool        m_cacheScans;   // Fullscans Writes to cache
	k3entity_cache* m_cache;
	list<k3entityIndex*>  m_indexes;
	list<k3entityDepends*> m_depencies;

	//UID		generateUID(k3obj);
	//bool	fullscanReader(const UID &uid, k3obj& obj);
	//bool	fullscanRemove(const UID &uid);
	//bool	forEachHelper(wstring strtwith, k3foreachfunc, void*);
    //bool	forEachHelperKeyRange(wstring fpath, k3foreachfunc k3func, void* param, K3KEY begin, K3KEY end);
	bool	forEachHelperKeyRange(const K3KEY &begin, const K3KEY &end, k3foreachfunc k3func, void* param = NULL, wstring byKeyOrder = L"");
	HANDLE	getTransactHndl();
    k3entityIndex* getIndex(const wstring &key);
    bool    hasDepencies();
    void    updateDepencies(const k3obj&);
    void    updateDepencies();
	static bool fsCountFunctor(k3obj *obj, void* param);

public:
	int			m_blocksize;	// default (minimal) in-memory datablock size
	k3diskgroup*    m_disk;
	k3entity(k3trans* tr, const wstring &name, const k3obj& example);

	virtual ~k3entity();
	// object operations
	//void	add(k3obj&);
	bool	load(const UID &uid, k3obj &obj);
	bool	load(const k3objheader &oh, k3obj &obj);
	bool	load(const K3KEY &key, UIDvector &ovec, const wstring &keyName=L"PRIMARY");
	bool	loadUni(const K3KEY &key, k3obj &obj, const wstring &keyName=L"PRIMARY"); // throw on non unique
	bool	save(k3obj &obj);  // true if addnew, uniquieness not checked
	void	remove(const k3objheader &oh);
	void	remove(const UID &uid);
	void	remove(const K3KEY &key, const wstring &keyName=L"PRIMARY");

	//bool	load(const k3link&, UIDvector&);  ??? may be in transaction

	// bool	exists(const UID&);
	// bool	exists(const K3KEY&);

	virtual void	hash(const K3KEY&, K3KEY &ret);	// obj->filename mapping (1st key in file)
	wstring	hashFName(const K3KEY&); // return "0000000\0567.k3d"
	bool	forEach(k3foreachfunc, void* param = NULL, wstring byKeyOrder = L"PRIMARY");
        // return true if breaked (dont'use cache), default order - primary key
	bool	forEachKeyRange(const K3KEY &begin, const K3KEY &end, k3foreachfunc k3func, void* param = NULL, wstring byKeyOrder = L"PRIMARY");

// entity operations
	void    flush();        // cache fsync
    void    discard();      // cache drop in rollback
/*
	k3entity&	operator=(const k3entity &); // copy
	k3entity&	intersect(k3entity);  // A && B
	k3entity&	subset(const K3KEY &begin, const K3KEY &end, bool functor(obj&)); 
	k3entity&	union(k3entity);  // A+B
	k3entity&	minus(const &k3entity); // A-B 
	k3entity&	cartesian(k3entity); // A*B
*/

	unsigned __int64		count();
	// self operations
	void	drop();
};


class k3entityDepends : public k3entity
/// Dependend entity with auto recount. Aggregates && OLAP base
{
    int             m_thres;    // how much we store in memory, before recount
    list<k3obj*>    m_templist; // not recounted
public:
    k3entity &m_master;
    k3entityDepends(k3entity &master, const wstring &name, const k3obj &example);
    virtual ~k3entityDepends();
    virtual void    regenerate() = 0;  // drop all && full recreate
    virtual void    regenerate(const k3obj&, bool added = false) = 0;
        // check regeneration need, add - appended object in entity (false = safe recount)
};

class k3indexLeaf : public k3obj
/// obj saved in indexes's entity
{
    friend class k3entity;
    friend class k3entityIndex;
public:
    k3objheader m_targetHdr;
    k3indexLeaf();
    virtual k3obj *create() const { return new k3indexLeaf(); }
    virtual wstring getPrimaryKeyName() const { return L"PRIMARY"; }
};

class k3entityIndex : public k3entityDepends
/// Index
{
	static bool fsTargetFunctor(k3obj *obj, void* param);
    static bool fsGenerateFunctor(k3obj *obj, void* param);

public:
    wstring m_keyname;
    k3entityIndex(k3entity& master, const wstring &keyname);
    bool	loadTarget(const K3KEY &key, UIDvector &ovec); // returns uid's of master's objects
    bool	loadUniTarget(const K3KEY &key, k3obj &obj); // returns master's object
    virtual void    regenerate();  // drop all && full recreate
    virtual void    regenerate(const k3obj&, bool added = false);  // check regenerate need
//    bool	forEachTarget(k3foreachfunc k3func, void* param);
    bool	forEachKeyRangeTarget(const K3KEY &begin, const K3KEY &end, k3foreachfunc k3func, void* param = NULL);
};

/*
class k3link : public k3obj
/// struct for storing temporary on-disk link
{
public:
    UID     m_target;
    wstring m_entity;   // name
    k3link(K3KEY key, UID target): m_target(target)
    {
        d.m_prikey = key;
        d.m_size += sizeof(UID);
        CoCreateGUID(&d.m_uid);
    }
    virtual k3obj *create(){ return new k3link(); }
	virtual wstring getPrimaryKeyName() { return L"KEY"; }

    k3obj*  load(k3trans*);     // return new allocated target object
    k3obj*  load(k3entity&);    // return new allocated target object
};
*/


class k3trans
/// transaction
{
	enum k3trans_isol {readcommited, repeatebleread, seriazable, dirtyread, snapshot};
	friend class k3entity_cache;
	friend class k3entity;
	friend class k3bufferblock;

	//static list<k3trans>	m_all;
	list<k3entity*>	        m_ents;

	k3trans_isol	m_isolation;
	HANDLE	m_transact;
	wstring	m_name;
	int		m_cachesize;
	bool    m_inProgress;
	void    registerEntity(k3entity* ent);
	void    deregisterEntity(k3entity* ent);
public:
	k3trans(wstring name, k3trans_isol is = readcommited);
	~k3trans();
	static  k3trans	*getTransaction(wstring name);
	void	start(k3trans_isol is = readcommited);
	void	setCachesize(int sz); // KBytes
	void	commit();
	void	rollback();
};



//================ Serialization ===================
template <>
void k3obj::serializeValue <wstring> (BYTE* bin, int maxsz, wstring m_str, int &newlen)
{
    int     size = sizeof(int) + sizeof(wchar_t) * (m_str.size() + 1);
    _ASSERT(newlen + size <= maxsz);

    if(newlen + size > maxsz)
        throw k3except(L"k3obj::serializeString() Overflow");

    if (serializeOn)
    {
        int len = m_str.size() + 1;
        serializeValue(bin, maxsz, len, newlen);
        memcpy(bin + newlen, m_str.c_str(), len * sizeof(wchar_t));
        newlen += len * sizeof(wchar_t);
    }
    else
        newlen += size;
}


template <class Type>
void k3obj::serializeValue(BYTE* bin, int maxsz, Type m_pod, int &newlen)
{
    int     size = sizeof(Type);
    _ASSERT(newlen + size <= maxsz);

    if(newlen + size > maxsz)
        throw k3except(L"k3obj::serializeValue() Overflow");

    if (serializeOn)
        memcpy(bin + newlen, &m_pod, size);

    newlen += size;
}


template <class Type>
void k3obj::serializeArray(BYTE* bin, int maxsz, vector<Type> m_vec, int &newlen)
{
	unsigned int len = m_vec.size();
    serializeValue(bin, maxsz, len, newlen);
    for(unsigned int i = 0; i < m_vec.size(); i++)
		serializeValue(bin, maxsz, m_vec[i], newlen);
}


template <class KeyType, class DataType>
void k3obj::serializeMap(BYTE* bin, int maxsz, map<KeyType, DataType> m_map, int &newlen)
{
	int len = m_map.size();
    serializeValue(bin, maxsz, len, newlen);
    for(map<KeyType, DataType>::iterator it = m_map.begin(); it != m_map.end(); it++)
    {
        serializeValue(bin, maxsz, it->first, newlen);
        serializeValue(bin, maxsz, it->second, newlen);
    }
}

//================ deserialization ===================
template <>
void k3obj::deserializeValue <wstring> (BYTE* bin, int maxsz, wstring &m_str, int &newlen)
{
    int     size = sizeof(int);
    _ASSERT(newlen + size <= maxsz);

    if(newlen + size > maxsz)
        throw k3except(L"k3obj::deserializeString() Overflow");

	deserializeValue(bin, maxsz, size, newlen);
    _ASSERT(newlen + size * (int)sizeof(wchar_t) <= maxsz);

    if(newlen + size * (int)sizeof(wchar_t) > maxsz)
        throw k3except(L"k3obj::deserializeString() Overflow");

	wchar_t*	pchar = reinterpret_cast<wchar_t*>(bin + newlen);

	_ASSERT(wcslen(pchar) == size - 1);
	_ASSERT(pchar[wcslen(pchar)] == '\0');
		
	m_str = pchar;
	newlen += size * (int)sizeof(wchar_t);
}


template <class Type>
void k3obj::deserializeValue(BYTE* bin, int maxsz, Type &m_pod, int &newlen)
{
    int     size = sizeof(Type);
    _ASSERT(newlen + size <= maxsz);

    if(newlen + size > maxsz)
        throw k3except(L"k3obj::deserializeValue() Overflow");

    memcpy(&m_pod, bin + newlen, size);

    newlen += size;
}


template <class Type>
void k3obj::deserializeArray(BYTE* bin, int maxsz, vector<Type> &m_vec, int &newlen)
{
    int     size = 0;
	deserializeValue(bin, maxsz, size, newlen);

	m_vec.clear();
	for(int i = 0; i < size; i++)
	{
		Type temp;
        deserializeValue(bin, maxsz, temp, newlen);
		m_vec.push_back(temp);
	}
}


template <class KeyType, class DataType>
void k3obj::deserializeMap(BYTE* bin, int maxsz, map<KeyType, DataType> &m_map, int &newlen)
{
    int     size = 0;
	deserializeValue(bin, maxsz, size, newlen);

	m_map.clear();
	for(int i = 0; i < size; i++)
	{
		KeyType		tempKey;
		DataType	tempVal;
        deserializeValue(bin, maxsz, tempKey, newlen);
        deserializeValue(bin, maxsz, tempVal, newlen);
		m_map.insert(pair<KeyType, DataType>(tempKey, tempVal));
	}
}


#endif


