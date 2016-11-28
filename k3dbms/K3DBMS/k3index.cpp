/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Classes k3entityIndex, k3entityDepends - dependent entities
 *
 * Modifications history:
 * 100428 Creation
 */

#include "stdafx.h"

using namespace std;
#include "k3dbms.h"

k3indexLeaf::k3indexLeaf()
    : k3obj()
{
    d.m_size = sizeof *this - ((BYTE*)&d - (BYTE*)this);
    // CoCreateGuid(&d.m_uid);  //! index leaf UID = master object UID

    memset(&m_targetHdr, 0, sizeof m_targetHdr);
	//d.m_prikey.key = d.m_uid;
}

//================================ realisation ==================================

k3entityDepends::k3entityDepends(k3entity &master, const wstring &name, const k3obj &example)
    : k3entity(master.m_trans, name, example), m_master(master)
{
    m_thres = 0;
}


k3entityDepends::~k3entityDepends()
{
	for(list<k3obj*>::iterator it = m_templist.begin(); it != m_templist.end(); it++)
        delete *it;
}

//================================ realisation ==================================

k3entityIndex::k3entityIndex(k3entity& master, const wstring &keyname)
    : k3entityDepends(master, master.m_name + wstring(L"_index") + wstring(L"_") + keyname,
                      k3indexLeaf()), m_keyname(keyname)
{
    m_cacheScans = true;
	master.m_indexes.push_back(this);
}


bool k3entityIndex::fsGenerateFunctor(k3obj *obj, void* param)
{
    k3entityIndex*    me = reinterpret_cast<k3entityIndex*>(param);

    k3indexLeaf leaf;
    leaf.d.m_uid = obj->d.m_uid;
    obj->getKey(me->m_keyname, leaf.d.m_prikey);
    leaf.m_targetHdr = obj->d;

    me->save(leaf);

    return false;
}


void    k3entityIndex::regenerate()
/// drop all && full recreate
{
    drop();
    m_master.forEach(fsGenerateFunctor, this);
}


void    k3entityIndex::regenerate(const k3obj &obj, bool added)
/// check regenerate need
{
    k3indexLeaf leaf;
    leaf.d.m_uid = obj.d.m_uid;
    obj.getKey(m_keyname, leaf.d.m_prikey);
    leaf.m_targetHdr = obj.d;
    save(leaf);
    // logic trouble - if key changed (to other block), old leaf is dead (orphan) link (but have same uid)
//    k3indexLeaf leaf2;
//	load(leaf.d, leaf2);
//	_ASSERTE(memcmp(&leaf, &leaf2, sizeof(leaf))==0);
}


struct k3targetFunctorData
{
	k3targetFunctorData(k3entityIndex* pme, k3foreachfunc func, void* ppar)
		: me(pme), k3func(func), param(ppar) {}
    k3entityIndex*    me;
    k3foreachfunc* k3func;
    void* param;
};


bool k3entityIndex::fsTargetFunctor(k3obj *obj, void* param)
{
    k3targetFunctorData* data = static_cast<k3targetFunctorData*>(param);
    k3indexLeaf*    leaf = dynamic_cast<k3indexLeaf*>(obj);

    UIDvector   ovec;
    k3objptr optr(data->me->m_master.m_factory->create());
    bool ret = false;

    bool fnd = data->me->m_master.m_cache->load(leaf->m_targetHdr, *optr.get());
    if (fnd)
    {
        // check orphanage = broken links
        K3KEY   tkey;
        optr->getKey(data->me->m_keyname, tkey);
        if (tkey == leaf->d.m_prikey && leaf->d.m_uid == optr->d.m_uid) // OK
        {
            ret = data->k3func(optr.get(), data->param);
        }
        else
            data->me->remove(leaf->d);   // orpan index leaf - key changed
    } else
    { // orpan index leaf - no master
        data->me->remove(leaf->d);
    }

    return ret;
}

/*
bool	k3entityIndex::forEachTarget(k3foreachfunc k3func, void* param)
{
	k3targetFunctorData data(this, k3func, param);
    return forEach(fsTargetFunctor, &data);
}
*/

bool	k3entityIndex::forEachKeyRangeTarget(const K3KEY &begin, const K3KEY &end, k3foreachfunc k3func, void* param)
{
	k3targetFunctorData data(this, k3func, param);
    return forEachKeyRange(begin, end, fsTargetFunctor, &data);
}


bool	k3entityIndex::loadTarget(const K3KEY &key, UIDvector &ovec)
 // returns uid's of master's objects
{
    UIDvector ovecIdx;
    bool rc = load(key, ovecIdx);
	bool res = false;  
    for (unsigned i = 0; i < ovecIdx.size(); i++)
    {
        k3indexLeaf leaf;
        rc = load(ovecIdx[i], leaf);  
        // check orphanage = broken links - impossible without load master
		if(rc)
		{
			k3obj*	pobj = m_master.m_factory;
			rc = m_master.load(leaf.m_targetHdr, *pobj);
			// rc = rc && (leaf.d.m_uid == pobj->d.m_uid);
			if (rc)
			{	
				ovec.push_back(leaf.m_targetHdr);
				res = true;
			}
			else // debug
				rc = m_master.load(leaf.m_targetHdr, *pobj);


		}
    }

    return res;
}


bool	k3entityIndex::loadUniTarget(const K3KEY &key, k3obj &obj)
// returns master's object
{
    k3indexLeaf leaf;
    bool rc = loadUni(key, leaf);

    if (rc)
	{        // check orphanage = broken links
        rc = m_master.load(leaf.m_targetHdr, obj);
		// rc = rc && (leaf.d.m_uid == obj.d.m_uid);
	}

    return rc;
}

