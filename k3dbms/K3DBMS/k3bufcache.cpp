/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Class k3bufcache - caching system
 *
 * Modifications history:
 * 100429 - remove k3objlist logic
 * 100509 - forEach logic strained, refactoring
 * 100510 - change load/save interface
 * 100521 - speed up - new writemiss logic
 * 100524 - more precise cache size in sweep()
 */

#include "stdafx.h"

using namespace std;

#include "k3dbms.h"




//================================ realisation ==================================
k3buffercache_map	k3entity_cache::buffercache;
LPCRITICAL_SECTION	k3entity_cache::k3bufferCritSect = NULL;
int     k3entity_cache::k3bufferRefcount = 0;

k3entity_cache::k3entity_cache(k3entity& e) :
	m_entity(e)
{  // CACHEWRITER

    // create if none. refcount
    if (k3bufferRefcount++ <= 0)
	{ // this block is not 100% thread safe, sorry =)
	    _ASSERT(!k3bufferCritSect);
		k3bufferCritSect = new CRITICAL_SECTION;
#ifdef ACID
		InitializeCriticalSectionAndSpinCount(k3bufferCritSect, 0x80000400);
#else
		InitializeCriticalSection(k3bufferCritSect);
#endif
	}

	k3CriticalSection	sect(k3bufferCritSect);

	wstring trans_name;


	// workaround if nontransacted. auto create it
	if (m_entity.m_trans == NULL)
	{
		trans_name = L"NONTRANSACTED";

	}else
	{ // get ref to our transaction cache
		trans_name = m_entity.m_trans->m_name;
	}

	k3buffercache_map::iterator it_tr;

	it_tr = buffercache.find(trans_name);
	if (it_tr == buffercache.end())
	{
		k3transcache_map	m;
		buffercache.insert(k3buffercache_map::value_type(trans_name, m));
		it_tr = buffercache.find(trans_name);
	}
	_ASSERT(it_tr != buffercache.end());	// must be created

    // share entity cache within one transaction
	k3transcache_map::iterator it_ent;
	k3transcache_map  &trmap = it_tr->second;
	it_ent = trmap.find(m_entity.m_name);
	if (it_ent != trmap.end())
	{
		m_cache = &it_ent->second;
	}else	// no such entity used before in that transaction
	{
		k3entitycache_map	em;
		trmap.insert(k3transcache_map::value_type(m_entity.m_name, em));
		it_ent = trmap.find(m_entity.m_name);
		m_cache = &it_ent->second;
	}
}


k3entity_cache::~k3entity_cache()
{  // save blocks & clear
	try
	{
		flush();
		discard();  // if entity opened twice or more in one transaction, lost performance
	}
	catch(k3except &xc)
	{
		//cerr << "k3entity_cache::~k3entity_cache() critical error explained next line" << endl;
		//wcout << xc.s_error << endl;
		_ASSERT(0);
	}

    if (--k3bufferRefcount <= 0)
	{
	    _ASSERT(k3bufferCritSect);
		DeleteCriticalSection(k3bufferCritSect);
		delete k3bufferCritSect;
		k3bufferCritSect = NULL;
	}
}


void	k3entity_cache::flush()
{// save blocks
// CACHEWRITER
	k3CriticalSection	sect(k3bufferCritSect);

	k3entitycache_map::iterator it;
	for (it = m_cache->begin(); it != m_cache->end(); it++)
	{
		k3bufferblock &bl = it->second;

		if (bl.m_state == k3bufferblock::dirty || 
			bl.m_state == k3bufferblock::reserved)
		{
			bl.write();
			bl.m_state = k3bufferblock::valid;
			m_statistics += k3stat(0, 1, 0, 1);
			m_entity.m_disk->m_statistics += k3stat(0, 1, 0, 1);
		}
	}
}


void	k3entity_cache::discard()
{// clear all when rollback
// CACHEWRITER
	k3CriticalSection	sect(k3bufferCritSect);

    // may be need invalidation all other??

    m_cache->clear();  // if entity opened twice or more in one transaction, lost performance
}


void	k3entity_cache::sweep()
{// CACHEWRITER
	k3CriticalSection	sect(k3bufferCritSect);

	int  maxsz = m_entity.m_trans ? m_entity.m_trans->m_cachesize : k3def_transaction_cache_size;
	maxsz *= 1024;
	int		used = 0;
/* bad logic
	int  used = m_cache->size() * m_entity.m_blocksize;  // incorrect. blocksize may be not real
*/
	k3entitycache_map::iterator it_bl, it_tmp;
	it_bl = m_cache->begin();
	if (it_bl != m_cache->end())
	{
		k3bufferblock &block = it_bl->second;
		used = block.m_size *  m_cache->size();  // take 1st block as example
	}

	if (used > maxsz)
	{ // clear up to 70% full
		int bullet = rand();
		for(; it_bl!= m_cache->end(); )
		{
			if (++bullet % 3 == 0)
			{
				k3bufferblock &block = it_bl->second;
				if (block.m_state == k3bufferblock::dirty ||
					block.m_state == k3bufferblock::reserved)
				{
					block.write();
					m_statistics += k3stat(0, 1, 0, 1);
					m_entity.m_disk->m_statistics += k3stat(0, 1, 0, 1);
				}
				it_tmp = it_bl++;
				m_cache->erase(it_tmp);   // in DMC++ STLport erase() return void
			}
			else
				it_bl++;
		}

	}
}


void k3entity_cache::walkall_readmiss(const wstring &ent, const K3KEY &key1st)
// coherence helper
{
	k3buffercache_map::iterator it_tr;
	for(it_tr = buffercache.begin(); it_tr!= buffercache.end(); it_tr++)
	{
		k3transcache_map::iterator it_ent;
		k3transcache_map  &trmap = it_tr->second;
		it_ent = trmap.find(ent);
		if (it_ent == trmap.end()) break;	// no such entity used in that transaction
		k3entitycache_map::iterator it_bl;
		k3entitycache_map &bl_map = it_ent->second;

		it_bl = bl_map.find(key1st);
		if (it_bl == bl_map.end()) break;	// no such block cached

		k3bufferblock &block = it_bl->second;

		switch (block.m_state)
		{
		case k3bufferblock::dirty:
			block.write();		// check if there is dirty block
		case k3bufferblock::reserved:
			block.m_state = k3bufferblock::valid;
			break;
		case k3bufferblock::valid:
		case k3bufferblock::invalid:
		default:
			break;
		}

	}

}


void k3entity_cache::walkall_invalidate(const wstring &ent, const K3KEY &key1st)
// coherence helper
{
	k3buffercache_map::iterator it_tr;
	for(it_tr = buffercache.begin(); it_tr!= buffercache.end(); it_tr++)
	{
		k3transcache_map::iterator it_ent;
		k3transcache_map  &trmap = it_tr->second;
		it_ent = trmap.find(ent);
		if (it_ent == trmap.end()) break;	// no such entity used in that transaction
		k3entitycache_map::iterator it_bl;
		k3entitycache_map &bl_map = it_ent->second;

		it_bl = bl_map.find(key1st);
		if (it_bl == bl_map.end()) break;	// no such block cached

		k3bufferblock &block = it_bl->second;
		block.m_state = k3bufferblock::invalid;
	}
}


k3bufferblock*    k3entity_cache::loadBlock(const K3KEY &key1st, bool caching)
// coherence check. all need thread safety
{// CACHEWRITER
	k3CriticalSection	sect(k3bufferCritSect);

	bool	blockOK = false, blockCached = false;
	k3bufferblock*	retval = NULL;
	//K3KEY	key1st(0);
    //m_entity.hash(key, key1st);
    //_ASSERT(key == key1st);

	k3entitycache_map::iterator it;
	it = m_cache->find(key1st);

    blockCached = it != m_cache->end();
	if (blockCached)
	{ // read hit (block in cache)
		k3bufferblock& block = it->second;
		blockOK = block.m_state != k3bufferblock::invalid;
		block.m_lru++;

		if (blockOK) 
			retval = &block;

	};

	if (!blockOK || !blockCached)
	{
		// block read miss
		walkall_readmiss(m_entity.m_name, key1st);  // save this block from all cache if dirty
			/*
			if (reserved) = set valid;
			if (dirty) writedirty;
			wait dirty sync, then read
			*/
        if (blockCached)
        {// reload invalid
            k3bufferblock &block = it->second;
            block.read();
            block.m_state = k3bufferblock::valid;
            retval = &block;
        }else
        {
            k3bufferblock block(m_entity, key1st);
            block.read();
            block.m_state = k3bufferblock::valid;
            if (caching)
            {
	            sweep();    // clear old with LRU algorithm
                m_cache->insert(k3entitycache_map::value_type(key1st, block));
				retval = &m_cache->find(key1st)->second;
			}
        }
	}

	m_statistics += k3stat(1, 0, !blockOK, 0);
	m_entity.m_disk->m_statistics += k3stat(1, 0, !blockOK, 0);

	return retval;
}


void    k3entity_cache::updateBlock(k3bufferblock* bl, bool isnew)
// coherence check. need thread safe
{// CACHEWRITER !!
	k3CriticalSection	sect(k3bufferCritSect);

	//_ASSERT(obj.d.m_prikey.key > 0);
	//K3KEY	key1st(0);
	//m_entity.hash(obj.d.m_prikey, key1st);

	k3entitycache_map::iterator it;
	it = m_cache->find(bl->m_key1st);

	int	phiswrite = 0;

	bool blockCached = it != m_cache->end();
	_ASSERT(blockCached);   // valid only for cache

	k3bufferblock& block = it->second;
	_ASSERT(&block == bl);
	if (&block != bl)
		throw k3except(L"k3entity_cache::updateBlock() Cached block differs from original. May result from error in k3entity::hashXXX()");

	// new writemiss logic
	blockCached &= !isnew; 

	if (blockCached)
	{ // write hit (block in cache)
		block.m_lru++;
		switch (block.m_state)
		{
		case k3bufferblock::dirty:
		case k3bufferblock::reserved:
			block.m_state = k3bufferblock::dirty;
			break;
		case k3bufferblock::valid:
			walkall_invalidate(m_entity.m_name, bl->m_key1st);
			block.m_state = k3bufferblock::dirty;
			block.write();
			phiswrite = 1;
			block.m_state = k3bufferblock::reserved;
			break;
		case k3bufferblock::invalid:
			_ASSERT(0); // impossible. Write called only for valid blocks 
/*
			loadBlock(key1st, block, true);  // update block in cache. state => valid
			block.m_state = k3bufferblock::dirty;
*/
			break;
		default:
            _ASSERT(0);
			break;
		}
	}else
	{
		// new writemiss logic
		block.m_state = k3bufferblock::reserved;


/*      // old writemiss logic  
		bl.m_state = k3bufferblock::dirty;
        bl.write();
        phiswrite = 1;
        bl.m_state = k3bufferblock::reserved;

        if (caching)
        {
            sweep();    // clear old with LRU algorithm
            m_cache->insert(k3entitycache_map::value_type(key1st, bl));
        }
*/
    }

	m_statistics += k3stat(0, 1, 0, phiswrite);
	m_entity.m_disk->m_statistics += k3stat(0, 1, 0, phiswrite);
}


struct loadFunctorData
{
    loadFunctorData(const UID &puid, k3obj &pobj)
        : uid(puid), target(pobj) {};
    const UID &uid;
    k3obj &target;
};


bool    k3entity_cache::loadFunctor(k3obj* pobj, void* param)
{
    loadFunctorData* pData = reinterpret_cast<loadFunctorData*>(param);
    if (pobj->d.m_uid == pData->uid)
    {
        pData->target = *pobj;
        return true;
    }
    return false;
}


bool	k3entity_cache::load(const UID &uid, k3obj &obj)
// full scan. checking need of FS is in entity
{
    loadFunctorData data(uid, obj);
    return forEachKeyRange(K3KEY(), K3KEY(), loadFunctor, &data);
}


bool	k3entity_cache::load(const k3objheader &oh, k3obj &obj)
{
	K3KEY	key1st;
    m_entity.hash(oh.m_prikey, key1st);

	k3CriticalSection	sect(k3bufferCritSect);

    k3bufferblock   *block = loadBlock(key1st, true);
	bool rc = block ? block->load(oh, obj) : false;
	
	return rc;
}


bool	k3entity_cache::load(const K3KEY &key, UIDvector &ovec)
{
	K3KEY	key1st;
    m_entity.hash(key, key1st);

	k3CriticalSection	sect(k3bufferCritSect);

    k3bufferblock   *block = loadBlock(key1st, true);
	bool rc = block ? block->load(key, ovec) : false;
	
	return rc;
}


bool	k3entity_cache::loadUni(const K3KEY &key, k3obj &obj)
{
	K3KEY	key1st;
    m_entity.hash(key, key1st);

	k3CriticalSection	sect(k3bufferCritSect);

    k3bufferblock   *block = loadBlock(key1st, true);
	bool rc = block ? block->loadUni(key, obj) : false;
	
	return rc;
}


void	k3entity_cache::remove(const UID &uid)
{
    bool   rc = load(uid, *m_entity.m_factory);
    if (rc) remove(m_entity.m_factory->d);
}


void	k3entity_cache::remove(const k3objheader &oh)
{
	K3KEY	key1st;
    m_entity.hash(oh.m_prikey, key1st);

	k3CriticalSection	sect(k3bufferCritSect);

    k3bufferblock   *block = loadBlock(key1st, true);
	if (block)
	{
		block->remove(oh);
		updateBlock(block);
	}
}


void	k3entity_cache::remove(const K3KEY &key)
{
	K3KEY	key1st;
    m_entity.hash(key, key1st);

	k3CriticalSection	sect(k3bufferCritSect);

    k3bufferblock   *block = loadBlock(key1st, true);
	if (block)
	{
		block->remove(key);
		updateBlock(block);
	}
}


bool	k3entity_cache::save(k3obj &obj)
{
	K3KEY	key1st;
    m_entity.hash(obj.d.m_prikey, key1st);
	bool rc = false;

	k3CriticalSection	sect(k3bufferCritSect);

	k3bufferblock   *block = loadBlock(key1st, true);
	_ASSERT(block);  //  must be new or old
	_ASSERT(key1st == block->m_key1st);
	if (block)
	{
		// block is void & clean => its new created => write miss
		// new writemiss logic
		bool isnew = (block->m_objoffsets.size() == 0);
			//&& block->m_state != k3bufferblock::dirty); 
		rc = block->save(obj);
		try
		{
			updateBlock(block, isnew);
		}
		catch(...)  // on error block is in inconsistent state with other transactions
		{
			block->m_state = k3bufferblock::invalid;
			throw;
		}

	}

	return rc;
}


/*
bool	k3entity_cache::load(const K3KEY &key, UIDvector &ovec, const k3objheader *pObjh, k3obj *pObj)
// coherence check. all need thread safety
{// CACHEWRITER
	EnterCriticalSection(k3bufferCritSect);

	int	fnd_blk = 0;
	bool	fnd_obj = false;
	K3KEY	key1st(0);
    m_entity.hash(key, key1st);

	k3entitycache_map::iterator it;
	it = m_cache->find(key1st);

	if (it != m_cache->end())
	{ // read hit (block in cache)
		k3bufferblock& block = it->second;
		fnd_blk = 1;
		block.m_lru++;

		if (block.m_state != k3bufferblock::invalid)
		{
		    if (!pObjh || !pObj) // load all vector
                fnd_obj = block.load(key, ovec);
            else
            {
                fnd_obj = block.load(*pObjh, *pObj);
                if (fnd_obj)
                    ovec.push_back(pObjh->m_uid);
            }
		}
	};

	if (!fnd_blk || !fnd_obj)   // ! fnd obj - if block was invalid
	{
		// block read miss
		walkall_readmiss(m_entity.m_name, key1st);  // save this block from all cache if dirty
		k3bufferblock *block;
		k3bufferblock tmpblock(m_entity, key1st);
		if (fnd_blk)
			block = &it->second;
		else
			block = &tmpblock;
		block->read();
		block->m_state = k3bufferblock::valid;

        if (!pObjh || !pObj) // load all vector
            fnd_obj = block->load(key, ovec);
        else
        {
            fnd_obj = block->load(*pObjh, *pObj);
            if (fnd_obj)
                ovec.push_back(pObjh->m_uid);
        }

		if (!fnd_blk)
		{
			m_cache->insert(k3entitycache_map::value_type(key1st, *block));
		}

		// clear old with LRU algorithm
		sweep(); // if memory or every 10th way . may delete last insert

	}

	m_statistics += k3stat(1, 0, !fnd_blk, 0);
	m_entity.m_disk->m_statistics += k3stat(1, 0, !fnd_blk, 0);

	LeaveCriticalSection(k3bufferCritSect);

	return	fnd_obj;
}


bool	k3entity_cache::save(k3obj &obj)
// coherence check. need thread safe
{// CACHEWRITER !!
	EnterCriticalSection(k3bufferCritSect);

	_ASSERT(obj.d.m_prikey.key > 0);
	K3KEY	key1st(0);
	m_entity.hash(obj.d.m_prikey, key1st);

	k3entitycache_map::iterator it;
	it = m_cache->find(key1st);
	bool saved = false;
	bool addnew = false;
	int	phiswrite = 0;

	if (it != m_cache->end())
	{ // write hit (block in cache)
		k3bufferblock& block = it->second;
		block.m_lru++;
		switch (block.m_state)
		{
		case k3bufferblock::dirty:
		case k3bufferblock::reserved:
			addnew = block.save(obj);
			block.m_state = k3bufferblock::dirty;
			saved = true;
			break;
		case k3bufferblock::valid:
			addnew = block.save(obj);
			walkall_invalidate(m_entity.m_name, key1st);
			block.m_state = k3bufferblock::dirty;
			block.write();
			phiswrite = 1;
			block.m_state = k3bufferblock::reserved;
			saved = true;
			break;
		case k3bufferblock::invalid:
		default:
			break;
		}
	};

	if (!saved)
	{
		UIDvector	ovec;
        //load(obj.d.m_prikey, ovec);
        addnew = save(obj);  // recursion, but must be only 1 level
	}

	m_statistics += k3stat(0, 1, 0, phiswrite);
	m_entity.m_disk->m_statistics += k3stat(0, 1, 0, phiswrite);

	LeaveCriticalSection(k3bufferCritSect);

	return addnew;
}


void	k3entity_cache::remove(const K3KEY &key, const k3objheader* objh)
{// CACHEWRITER
	EnterCriticalSection(k3bufferCritSect);

	K3KEY		key1st(0);
	m_entity.hash(key, key1st);

	k3entitycache_map::iterator it;
	it = m_cache->find(key1st);
	bool saved = false;
	int	phiswrite = 0;

	if (it != m_cache->end())
	{ // write hit (block in cache)
		k3bufferblock& block = it->second;
		block.m_lru++;
		switch (block.m_state)
		{
		case k3bufferblock::dirty:
		case k3bufferblock::reserved:
            if (!objh)
                block.remove(key);
            else
                block.remove(*objh);
			block.m_state = k3bufferblock::dirty;
			saved = true;
			break;
		case k3bufferblock::valid:
            if (!objh)
                block.remove(key);
            else
                block.remove(*objh);
			walkall_invalidate(m_entity.m_name, key1st);
			block.write();
			block.m_state = k3bufferblock::reserved;
			phiswrite = 1;
			saved = true;
			break;
		case k3bufferblock::invalid:
		default:
			break;
		}
	};

	if (!saved)
	{
		UIDvector	ovec;
        load(key, ovec, objh, NULL);
        remove(key, objh);  // recursion, but must be only 1 level
	}

	m_statistics += k3stat(0, 1, 0, phiswrite);
	m_entity.m_disk->m_statistics += k3stat(0, 1, 0, phiswrite);

	LeaveCriticalSection(k3bufferCritSect);
}


bool	k3entity_cache::cachescanReader(const UID &uid, k3obj& obj)
// true if found. cache-only operation
{
	EnterCriticalSection(k3bufferCritSect);

	int	fnd_blk = 0;
	bool	fnd_obj = false;

	k3entitycache_map::iterator it;
	for (it = m_cache->begin(); it != m_cache->end(); it++)
	{
		k3bufferblock& block = it->second;
		fnd_obj = block.load(uid, obj);
		if (fnd_obj)
		{
			fnd_blk = 1;
			block.m_lru++;
			break;
		}
	}

	m_statistics += k3stat(1, 0, !fnd_blk, 0);
	m_entity.m_disk->m_statistics += k3stat(1, 0, !fnd_blk, 0);

	LeaveCriticalSection(k3bufferCritSect);

	return	fnd_obj;
}


bool	k3entity_cache::cachescanRemove(const UID &uid)
// true if delete. cache-only operation
{
	EnterCriticalSection(k3bufferCritSect);

	k3entitycache_map::iterator it;
	bool saved = false, fnd_obj = false;
	int	phiswrite = 0;

	for (it = m_cache->begin(); it != m_cache->end(); it++)
	{
		k3bufferblock& block = it->second;
		fnd_obj = block.find(uid) >= 0;
		if (fnd_obj)	break;
	}

	if (it != m_cache->end())  // fnd_obj
	{ // write hit (block in cache)
		k3bufferblock& block = it->second;
		block.m_lru++;
		switch (block.m_state)
		{
		case k3bufferblock::dirty:
		case k3bufferblock::reserved:
			block.remove(uid);
			block.m_state = k3bufferblock::dirty;
			saved = true;
			break;
		case k3bufferblock::valid:
			block.remove(uid);
			walkall_invalidate(m_entity.m_name, block.m_key1st);
			block.write();
			block.m_state = k3bufferblock::reserved;
			phiswrite = 1;
			saved = true;
			break;
		case k3bufferblock::invalid:
		default:
			fnd_obj = false;
			break;
		}
	};

	m_statistics += k3stat(0, 1, 0, phiswrite);
	m_entity.m_disk->m_statistics += k3stat(0, 1, 0, phiswrite);

	LeaveCriticalSection(k3bufferCritSect);

	return fnd_obj;
}
*/

struct forEachKeyRangeFunctorData
{
	forEachKeyRangeFunctorData(k3entity_cache* pme, const K3KEY &pbegin, const K3KEY &pend, k3foreachfunc pfunc, void* ppar)
		: me(pme), begin(pbegin), end(pend), k3func(pfunc), param(ppar) {}

    k3entity_cache* me;
    const K3KEY &begin, &end;
    k3foreachfunc* k3func;
    void* param;
};


bool    k3entity_cache::forEachKeyRangeFunctor(const wstring &blname, void* param)
{
    forEachKeyRangeFunctorData*  pData = reinterpret_cast<forEachKeyRangeFunctorData*>(param);
	k3entity_cache* me = pData->me;
    k3bufferblock   *block,
                    copyblock(me->m_entity, K3KEY());
    bool    blockValid = false;

	k3CriticalSection	sect(k3bufferCritSect);

	{// must sync with cache, with or without m_cacheScans

        // lookup, is in cache block, hash(_prikey) == blname
        k3entitycache_map::iterator it;
        for (it = me->m_cache->begin(); it != me->m_cache->end(); it++)
        {
            const K3KEY &blkey = it->first;

			wstring	ename = me->m_entity.m_name + L"\\" + me->m_entity.hashFName(blkey);

            if (ename == blname)
            {
                block = me->loadBlock(blkey, true);
                blockValid = true;
                break;
            }
        }
    }

    if (!blockValid)
    // if not in cache, read copy from disk
    {
		copyblock.readBack(blname);
		walkall_readmiss(me->m_entity.m_name, copyblock.m_key1st);  // save this block from all cache if dirty
		copyblock.readBack(blname);  // reread after cache sync

		copyblock.m_state = k3bufferblock::valid;
		me->m_statistics += k3stat(1, 0, 1, 0);
		me->m_entity.m_disk->m_statistics += k3stat(1, 0, 1, 0);
		
		if (me->m_entity.m_cacheScans)
		{ // add loaded to cache
            me->sweep();    // clear old with LRU algorithm
            me->m_cache->insert(k3entitycache_map::value_type(copyblock.m_key1st, copyblock));
			block = &me->m_cache->find(copyblock.m_key1st)->second;
			blockValid = true;
		}
    }

	if(!blockValid)
		block = &copyblock;  // not updatable

    bool ret = block->forEachKeyRange(pData->begin, pData->end, pData->me->m_entity.m_factory,
            pData->k3func, pData->param);

/*
    if (block.m_objoffsets.size() != copyblock.m_objoffsets.size()
        || memcmp(block.m_buffer, copyblock.m_buffer, block.m_used_sz))
    {
        // block changed - invalidate cache or save to disk, whether m_cacheScans
        _ASSERT(0); // check if saved in this block, but why - must in k3entity
        pData->me->saveBlock(block.m_key1st, block, pData->me->m_entity.m_cacheScans);
    }
*/
    return ret;
}


bool	k3entity_cache::forEachKeyRange(const K3KEY &begin, const K3KEY &end, k3foreachfunc k3func, void* param)
{
    forEachKeyRangeFunctorData pfParam(this, begin, end, k3func, param);
    wstring beginFName, endFName;

	if (!begin.isNull)
		beginFName = m_entity.hashFName(begin);
	if (!end.isNull)
	    endFName = m_entity.hashFName(end);
    return m_entity.m_disk->forEachBlockRange(m_entity.m_name, forEachKeyRangeFunctor, &pfParam,
                                                beginFName, endFName, m_entity.getTransactHndl());
}


void k3entity_cache::cleanStatics()  
// call on exit app
{
	buffercache.clear();
}
