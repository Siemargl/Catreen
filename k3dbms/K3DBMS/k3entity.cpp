/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Class k3entity - object storage
 *
 * Modifications history:
 * 100429 - remove k3objlist logic
 * 100429 - add indexing && depencies
 * 100510 - change load/save interface
 */

#include "stdafx.h"

using namespace std;
#include "k3dbms.h"



//================================ realisation ==================================
k3entity::k3entity(k3trans* tr, const wstring &name, const k3obj& example)
{
    #pragma message("TODO: Replace hardcoded disk && path")
	static k3diskgroup diskc(L".\\DATA");
	m_disk = &diskc;

    m_cacheScans = false;

	m_name = name;
	m_trans = tr;

	m_disk->createEntity(m_name, getTransactHndl());

	// if no exception, create dynamics
	m_factory = example.create();
	m_prikey = m_factory->getPrimaryKeyName();

#pragma message("TODO: Correct obj_size as desire for variable size objects")
	m_objsize = m_factory->getsize();
	
	m_blocksize = k3def_block_size;
	if (m_objsize > m_blocksize) m_blocksize = m_objsize;

	m_cache = new k3entity_cache(*this);

    if (m_trans)
        m_trans->registerEntity(this);

    #pragma message("TODO: Metadata read && depencies open")

}


k3entity::~k3entity()
{
	delete m_factory;
	delete m_cache;

    if (m_trans)
        m_trans->deregisterEntity(this);
}


void	k3entity::flush()
{
    m_cache->flush();
}


void	k3entity::discard()
{
    m_cache->discard();
}


bool	k3entity::load(const UID &uid, k3obj& obj)
{
	bool fnd = false;
	k3entityIndex* idx = NULL;

	if (m_prikey == L"UID")
	{  // entity ordered by UID
		k3objheader objh;
		objh.m_prikey = objh.m_uid = uid;
        fnd = m_cache->load(objh, obj);
	} 
	else if (idx = getIndex(L"UID"))
	{ // load by index
        k3indexLeaf idxLeaf;
        fnd = idx->loadUni(K3KEY(uid), idxLeaf);
        if (fnd)
        {
            fnd = m_cache->load(idxLeaf.m_targetHdr, obj);
            _ASSERT(fnd);  // must exist becidxLeafause index exist
        }
	} 
	else
	{ // worst case = fullscan
		fnd = m_cache->load(uid, obj);
	}

	return fnd;
}


bool	k3entity::load(const k3objheader &oh, k3obj &obj)
{
    return m_cache->load(oh, obj);
}


bool	k3entity::load(const K3KEY &key, UIDvector &ovec, const wstring &keyName)
{
    bool fnd = false;
	k3entityIndex* idx = NULL;

	if (keyName == m_prikey || keyName == L"PRIMARY")
    {
        return m_cache->load(key, ovec);
    }
	else if (idx = getIndex(keyName))
	{ // load by index
        fnd = idx->loadTarget(key, ovec);
	} 
	else
	{ // key unknown
		throw k3except(L"k3entity::load() Unknown key(index)");
	}

	return fnd;
}


bool	k3entity::loadUni(const K3KEY &key, k3obj &obj, const wstring &keyName)
{
    bool fnd = false;
	k3entityIndex* idx = NULL;

    if (keyName == m_prikey || keyName == L"PRIMARY")
    {
        return m_cache->loadUni(key, obj);
    }
	else if (idx = getIndex(keyName))
	{ // load by index
        fnd = idx->loadUniTarget(key, obj);
	} 
	else
	{ // key unknown
		throw k3except(L"k3entity::loadUni() Unknown key(index)");
	}

	return fnd;
}


void	k3entity::remove(const UID &uid)
{
	k3entityIndex* idx = NULL;

    // depencies....generate data
    k3objptr objToRegen; // = NULL;
    if (hasDepencies())
    {
        objToRegen.reset(m_factory->create());
        bool fnd = load(uid, *objToRegen);
        if (!fnd) return;  // no such object
    }

	if (m_prikey == L"UID")
	{  // entity ordered by UID
		k3objheader objh;
		objh.m_prikey = objh.m_uid = uid;
        m_cache->remove(objh);
	} 
	else if (idx = getIndex(L"UID"))
	{ // by index
        k3indexLeaf idxLeaf;
        bool fnd = idx->loadUni(K3KEY(uid), idxLeaf);
        if (fnd)
        {
            m_cache->remove(idxLeaf.m_targetHdr);
        }
	} 
	else
	{ // worst case = fullscan, trying find in cache first
		m_cache->remove(uid);
	}

    // depencies....delete
    if (objToRegen.get())
        updateDepencies(*objToRegen);
}

/*
bool fsUpdatefunctor(k3obj *obj, void* param)
{
    k3entity*    my = dynamic_cast<k3entity*>(param);

    my->updateDepencies(*obj);

    return false;
}
    // depencies
    forEachKeyRange(key, key, fsUpdatefunctor, this);
*/

void	k3entity::remove(const K3KEY &key, const wstring &keyName)
{
	k3entityIndex* idx = NULL;

    if (keyName == m_prikey || keyName == L"PRIMARY")
    {
        m_cache->remove(key);
        updateDepencies();
#pragma message("TODO: Optimization - reindex partial only for deleted")
    }
	else if (idx = getIndex(keyName))
	{ // load by index
	    UIDvector ovec;
        idx->loadTarget(key, ovec);
        for (unsigned i = 0; i < ovec.size(); i++) remove(ovec[i]);
        updateDepencies();  // already done
	} 
	else
	{ // key unknown
		throw k3except(L"k3entity::remove() Unknown key(index)");
	}
}


void	k3entity::remove(const k3objheader &objh)
{
    k3objptr objToRegen; // = NULL;
    if (hasDepencies())
    {
        objToRegen.reset(m_factory->create());

        bool fnd = m_cache->load(objh, *objToRegen);
        if (!fnd) return;  // no such object
    }

    m_cache->remove(objh);

    // depencies....delete
    if (objToRegen.get())
        updateDepencies(*objToRegen);
}


/*
bool	k3entity::exists(UID uid)
{
	k3objptr	obj(m_factory->create());
	bool	rc;
	try
	{
		rc = load(uid, *obj);
	}catch(...)
	{
		//delete obj;
		throw;
	}
	//delete obj;
	return rc;
}
*/


bool	k3entity::save(k3obj& obj)
{
	obj.d.m_size = obj.getsize(); // workaround TODO: correct stable

    bool added = m_cache->save(obj);

    updateDepencies(obj);

	return added;
}


void		k3entity::hash(const K3KEY &key, K3KEY &ret)
// obj->filename mapping (1st key in file)
// distribution objects between fileblocks
// for all  key1 >= key2  =>  must be true hash(key1)>=hash(key2)
// must be true hash(k) == hash(hash(k))
// may change for indexes - depends of density or for GUID key - very sparse
{
	if (key.key[1] != 0) // primary key is GUID or string or datime
	{  // very sparse - get 4 MSB
		unsigned __int64 res = key.key[1] & 0xFFFFFFFF00000000;
		ret.key[1] = res;
		ret.key[0] = 0;
	}
	else
	{
		int	keysPerFile = m_blocksize / (m_objsize + 20); // blockheader size = 20 bytes
		if (keysPerFile == 0) keysPerFile = 1;

//		_ASSERT(key.key[1] == 0);
		unsigned __int64	res = key.key[0] / keysPerFile;  // if key[1] != 0, rangescans will fail
		res = res * keysPerFile;

		ret = res;
		ret.key[1] = 0;
	}



}


wstring	k3entity::hashFName(const K3KEY &key)
// return "000000000000\0567.k3d"
// for all  key1 >= key2  =>  must be true hash(key1)>=hash(key2)
{
	K3KEY key1st(0);
	hash(key, key1st);

	_ASSERT(!(key1st.key[0] && key1st.key[1]));  // only 1 part

	unsigned __int64 uid = key1st.key[0] ? key1st.key[0] : key1st.key[1] / 0xFFFFFFFF;
	_ASSERT((uid & 0xFFFFFFFF00000000) == 0);

	unsigned __int64 uid_lo = uid & 0xFFFF;
///	__int64 uid_hi = (key1st.key  >> 48) + 1;  // right rshift for 64bit don't work (
	unsigned __int64 uid_hi = (uid  & 0xFFFF0000) / 65536;

	wchar_t	filename[33];
	swprintf(filename, 33, L"%.8I64X\\%.8I64X.k3d", uid_hi, uid_lo);
	return /*m_path + L"\\" + m_name + L"\\" +*/ filename;
}

/*
bool fsSearchUIDfunctor(k3obj *obj, void* param)
{
    k3obj*    toObj = reinterpret_cast<k3obj*>(param);
    if (obj->d.m_uid == toObj->d.m_uid)
    {
        *toObj = *obj;
        return true;
    }
    return false;
}


bool	k3entity::fullscanReader(const UID &uid, k3obj& obj)
{
    obj.d.m_uid = uid;
	return forEach(fsSearchUIDfunctor, &obj);
}


bool fsRemoveUIDfunctor(k3obj *obj, void* param)
{
    k3objheader*    toObjh = static_cast<k3objheader*>(param);
    if (obj->d.m_uid == toObjh->m_uid)
    {
        *toObjh = obj->d;
        return true;
    }
    return false;
}


bool	k3entity::fullscanRemove(const UID &uid)
{
    k3objheader objh;
    objh.m_uid = uid;
	bool fnd = forEach(fsRemoveUIDfunctor, &objh);
	if (fnd)
        m_cache->remove(objh.m_prikey, &objh);

	return fnd;
}
*/

HANDLE	k3entity::getTransactHndl()
{
	if (m_trans)
		return m_trans->m_transact;
	else
		return INVALID_HANDLE_VALUE;
}


bool	k3entity::forEach(k3foreachfunc k3func, void* param, wstring byKeyOrder)
{
    _ASSERT(&k3func);

    K3KEY dummy;
    return forEachKeyRange(dummy, dummy, k3func, param, byKeyOrder);
}


bool	k3entity::forEachKeyRange(const K3KEY &begin, const K3KEY &end, k3foreachfunc k3func, void* param, wstring byKeyOrder)
{
    _ASSERT(begin.isNull || end.isNull || begin < end || begin == end);
    _ASSERT(&k3func);
    k3entityIndex *idx = NULL;

	if (byKeyOrder == m_prikey || byKeyOrder == L"PRIMARY")
    {
        return m_cache->forEachKeyRange(begin, end, k3func, param);
    } 
	else if (idx = getIndex(byKeyOrder))
    { // by index
        _ASSERT(idx); // no index with that key
        if (idx)
            return idx->forEachKeyRangeTarget(begin, end, k3func, param);
        else
            return forEachKeyRange(begin, end, k3func, param);
    }
    else
		throw k3except(L"k3entity::forEachKeyRange() Unknown key(index)");
}


k3entityIndex* k3entity::getIndex(const wstring &key)
{
 	for(list<k3entityIndex*>::iterator it = m_indexes.begin(); it != m_indexes.end(); it++)
 	{
        k3entityIndex *idx = *it;
        if (idx->m_keyname == key) return idx;
 	}

 	return NULL;
}


bool    k3entity::hasDepencies()
{
    return (m_indexes.begin() != m_indexes.end() ||
        m_depencies.begin() != m_depencies.end());
}


void    k3entity::updateDepencies(const k3obj &obj)
{
 	for(list<k3entityIndex*>::iterator it = m_indexes.begin(); it != m_indexes.end(); it++)
        (*it)->regenerate(obj);
 	for(list<k3entityDepends*>::iterator it = m_depencies.begin(); it != m_depencies.end(); it++)
        (*it)->regenerate(obj);
}


void    k3entity::updateDepencies()
{
 	for(list<k3entityIndex*>::iterator it = m_indexes.begin(); it != m_indexes.end(); it++)
        (*it)->regenerate();
 	for(list<k3entityDepends*>::iterator it = m_depencies.begin(); it != m_depencies.end(); it++)
        (*it)->regenerate();
}


bool k3entity::fsCountFunctor(k3obj *obj, void* param)
{
    unsigned __int64 *    cnt = static_cast<unsigned __int64*>(param);

    (*cnt)++;

    return false;
}


unsigned __int64    k3entity::count()
{
    unsigned __int64  cnt = 0;

    forEach(fsCountFunctor, &cnt);

    return cnt;
}


void	k3entity::drop()
{
	discard();
	m_disk->dropEntity(m_name, getTransactHndl());

	updateDepencies();
}
