/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Class k3bufferblock - binary file store && part of caching system
 *
 * Modifications history:
 */

#include "stdafx.h"

using namespace std;

#include "k3dbms.h"




//================================ realisation ==================================
k3bufferblock::k3bufferblock(k3entity &ent, const K3KEY &key1)
	: m_entity(ent), m_key1st(key1)
{
	/*
	m_size	= m_entity.m_blocksize;
	*/
	m_size = 512; // try to economy cache and correct sweep()

	m_buffer = new BYTE[m_size];
	memset(m_buffer, 0, m_size);
	m_used_sz = 0;
	m_lru = 0;
	m_state = invalid;	// default state = invalid
}


k3bufferblock::k3bufferblock(const k3bufferblock& bb)
	: m_entity(bb.m_entity), m_key1st(bb.m_key1st)
{
	m_size	= bb.m_size;
	m_buffer = new BYTE[m_size];
	m_used_sz = bb.m_used_sz;
	memcpy(m_buffer, bb.m_buffer, m_used_sz);
	m_lru = bb.m_lru;
	m_state = bb.m_state;
	m_objoffsets = bb.m_objoffsets;
}


k3bufferblock::~k3bufferblock()
{
	if (m_buffer)
		delete m_buffer;
}


const k3bufferblock&	k3bufferblock::operator =(const k3bufferblock& bb)
{
//	static int perftest = 0;
//	perftest++;

	if (m_size < bb.m_used_sz)
	{
		if (m_buffer)
			delete m_buffer;
		m_buffer = new BYTE[bb.m_size];
		m_size = bb.m_size;
	}
	m_used_sz = bb.m_used_sz;
	memcpy(m_buffer, bb.m_buffer, m_used_sz);
	m_lru = bb.m_lru;
	m_state = bb.m_state;
	m_objoffsets = bb.m_objoffsets;

	return *this;
}


void    k3bufferblock::initKey1st()
// internal func, called when we init from rawdata
{
	_ASSERT(m_buffer);

    k3blkcontens::iterator it = m_objoffsets.begin();
	if(it != m_objoffsets.end())
	{
		_ASSERT(it->second >= 0 && it->second < m_used_sz);
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);

		m_entity.hash(objh->m_prikey, m_key1st);
	}
//	else
//        throw k3except(L"k3bufferblock::initKey1st(). Cannot init - no data");
}


int		k3bufferblock::find(const UID &s_uid)
// internal func, -1 if none, linear search
{
	_ASSERT(m_buffer);

	for(k3blkcontens::iterator it = m_objoffsets.begin(); it != m_objoffsets.end(); it++)
	{
		_ASSERT(it->second >= 0 && it->second < m_used_sz);
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
		if (objh->m_uid == s_uid) return it->second;
	}
	return -1;
}


int		k3bufferblock::find(const k3objheader &objf)
// internal func, -1 if none, multimap search
{
	_ASSERT(m_buffer);

	for(k3blkcontens::const_iterator it = m_objoffsets.find(objf.m_prikey);
            it != m_objoffsets.end() && it->first == objf.m_prikey; it++)
	{
		_ASSERT(it->second >= 0 && it->second < m_used_sz);
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
		if (objh->m_uid == objf.m_uid) return it->second;
	}

	return -1;
}


bool	k3bufferblock::load(const UID &uid, k3obj& obj)
// true if found
{
	_ASSERT(m_state != invalid);
	if (m_state == invalid) return false;

	int ofs = find(uid);
	if (ofs < 0) return false;

	k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + ofs);

	obj.deserialize(m_buffer + ofs, objh->m_size);

	return true;
}


bool	k3bufferblock::load(const K3KEY &key, UIDvector& ovec)
// true if found > 0. clears vector, then creates objs on heap
{
	_ASSERT(m_buffer);
	_ASSERT(m_state != invalid);

	bool	rc = false;

	for(k3blkcontens::iterator it = m_objoffsets.find(key);
		it != m_objoffsets.end() && it->first == key; it++)
	{
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
		ovec.push_back(*objh);
		rc = true;
	}

	return rc;
}


bool	k3bufferblock::loadUni(const K3KEY &key, k3obj &obj)	
// true if found 
{
	_ASSERT(m_buffer);
	_ASSERT(m_state != invalid);

	bool	rc = false;

	for(k3blkcontens::iterator it = m_objoffsets.find(key);
		it != m_objoffsets.end() && it->first == key; it++)
	{
		if (rc)
			throw k3except(L"k3bufferblock::loadUni() Result is not unique");

		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);

		obj.deserialize(m_buffer + it->second, objh->m_size);
		rc = true;
	}

	return rc;
}


bool	k3bufferblock::load(const k3objheader& objf, k3obj& obj)
// true if found > 0. clears vector, then creates objs on heap
{
	_ASSERT(m_buffer);
	_ASSERT(m_state != invalid);

	bool	rc = false;

	for(k3blkcontens::iterator it = m_objoffsets.find(objf.m_prikey);
		it != m_objoffsets.end() && it->first == objf.m_prikey; it++)
	{
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
		if (objh->m_uid == objf.m_uid)
		{
			obj.deserialize(m_buffer + it->second, objh->m_size);
            rc = true;
			break;
		}
	}

	return rc;
}


void	k3bufferblock::resize(int newsz)
{
	if (newsz > m_size)
	{
		BYTE *newbuf = new BYTE[newsz];
		memcpy(newbuf, m_buffer, m_size);
		delete m_buffer;
		m_buffer = newbuf;
		m_size = newsz;
	}
}


bool	k3bufferblock::save(k3obj &obj)
// addnew if need
{
	_ASSERT(m_state != invalid);
	bool added = false;

	int ofs = find(obj.d);
	if (ofs < 0) // new
	{
		int sz = obj.getsize();
		if (m_used_sz + sz > m_size)
		{
			int newsz =  m_size + sz + int(m_size * 0.3);	// +30%
			resize(newsz);
		}	// reallocation done
		obj.d.m_version++;
		obj.serialize(m_buffer + m_used_sz, sz);	// simply append to end
		m_objoffsets.insert(make_pair<K3KEY, size_t>(obj.d.m_prikey, m_used_sz));
		m_used_sz += sz;
		added = true;
	}else // found
	{
		int sz = obj.getsize();
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + ofs);
		if (sz <= objh->m_size)
		{ // enough old room
			obj.d.m_version++;
			obj.serialize(m_buffer + ofs, sz);
//			k3blkcontens::iterator it = m_objoffsets.find(obj.d);
//			_ASSERT(it != m_objoffsets.end());
//			it->second = sz;  // size correction
		}else
		{
			remove(obj.d);
			save(obj); // recursion 1-level
		}
	}

	return added;
}


void	k3bufferblock::remove(const k3objheader &objf)
// clears only contens map
{
	_ASSERT(m_state != invalid);
	for(k3blkcontens::iterator it = m_objoffsets.find(objf.m_prikey);
		it != m_objoffsets.end() && it->first == objf.m_prikey; it++)
	{
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
		if (objh->m_uid == objf.m_uid)
		{
			m_objoffsets.erase(it);
			break;
		}
	}
}


void	k3bufferblock::remove(const K3KEY &key)
// clears only contens map
{
	_ASSERT(m_state != invalid);
	m_objoffsets.erase(key);
}


void	k3bufferblock::remove(const UID &uid)
// clears only contens map
{
	int ofs = find(uid);

	if (ofs < 0) return;

	k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + ofs);

	m_objoffsets.erase(objh->m_prikey);
}


void k3bufferblock::read()
{
	_ASSERT(m_entity.m_disk);
	_ASSERT(!m_key1st.isNull);
	wstring ename = m_entity.m_name + L"\\" + m_entity.hashFName(m_key1st);

	readBack(ename);
}


void	k3bufferblock::readBack(const wstring &ename)
{
	m_entity.m_disk->reader(ename, *this, m_entity.getTransactHndl());

	// parse contens
	m_objoffsets.clear();

	int *num_of_contens = reinterpret_cast<int*>(m_buffer);
	pair<K3KEY, int> *item_contens = reinterpret_cast<pair<K3KEY, int>*>(m_buffer + sizeof(size_t));
	for (int i = 0; i < *num_of_contens; i++)
	{
		_ASSERT((BYTE*)(item_contens + i) < m_buffer + m_used_sz);
		m_objoffsets.insert(item_contens[i]);
	}

	initKey1st();
}


void k3bufferblock::write()
{
	_ASSERT(m_entity.m_disk);
	_ASSERT(!m_key1st.isNull);

	_ASSERT(m_state == dirty || m_state == reserved);
	if (m_state != dirty && m_state != reserved) return;

	// pack data
	size_t num_of_contens = m_objoffsets.size();
	pair<K3KEY, size_t> *item_contens;

	int contsize = sizeof(size_t) + m_objoffsets.size() * sizeof (*item_contens);
	int datasize = 0;
	BYTE * newbuf = new BYTE[max(contsize + m_used_sz, m_size)];
	BYTE * data = newbuf + contsize;
	item_contens = reinterpret_cast<pair<K3KEY, size_t>*>(newbuf + sizeof(size_t));
	*(size_t*)newbuf = m_objoffsets.size();

	for(k3blkcontens::iterator it = m_objoffsets.begin(); it != m_objoffsets.end(); it++)
	{
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
		memcpy(data + datasize, objh, objh->m_size);
		// offset changed in newbuf
		it->second = contsize + datasize;
		*item_contens = *it;

		datasize += objh->m_size;
		item_contens++;
	}

	delete m_buffer;
	m_buffer = newbuf;
	m_size = max(contsize + m_used_sz, m_size);
	m_used_sz = contsize + datasize;


	wstring fullpath = m_entity.m_name + L"\\" + m_entity.hashFName(m_key1st);

	m_entity.m_disk->writer(fullpath, *this, m_entity.getTransactHndl());
}


bool	k3bufferblock::forEach(k3foreachfunc k3func, void* param, k3obj* factory)
// return true if breaked.
{
	_ASSERT(m_buffer);
	_ASSERT(&k3func);

	bool	rc = false;

	for(k3blkcontens::iterator it = m_objoffsets.begin(); it != m_objoffsets.end(); it++)
	{
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
		factory->deserialize(m_buffer + it->second, objh->m_size);
		rc = k3func(factory, param);
		if (rc) break;
	}

	return rc;
}


bool	k3bufferblock::forEachKeyRange(const K3KEY &begin, const K3KEY &end, k3obj* factory, k3foreachfunc k3func, void* param)
// return true if breaked
{
	_ASSERT(m_buffer);
	_ASSERT(begin.isNull || end.isNull || begin < end || begin == end);
	_ASSERT(&k3func);
	_ASSERT(factory);

	bool	rc = false;

	k3blkcontens::const_iterator it = (begin.isNull || !(m_key1st < begin)) ?  
								m_objoffsets.begin() : m_objoffsets.find(begin);

	while(it != m_objoffsets.end() && (end.isNull || !(end < it->first)))
	{
		k3objheader *objh = reinterpret_cast<k3objheader*>(m_buffer + it->second);
        factory->deserialize(m_buffer + it->second, objh->m_size);
        rc = k3func(factory, param);
        if (rc) break;
		it++;
	}

	return rc;
}

