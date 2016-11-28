/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Class k3trans - transaction controller
 *
 * Modifications history:
 * 100429 - cache sync with transaction logic
 */

#include "stdafx.h"

using namespace std;
#include "k3dbms.h"

//================================ realisation ==================================
k3trans::k3trans(wstring name, k3trans_isol is)
{
	_ASSERT(name != L"");
	m_name = name;

	m_isolation = is;
	_ASSERTE(is == readcommited);

	m_cachesize = k3def_transaction_cache_size;
	m_transact = INVALID_HANDLE_VALUE;

    m_inProgress = false;

	k3buffercache_map::iterator it_tr;

	it_tr = k3entity_cache::buffercache.find(m_name);
	if (it_tr != k3entity_cache::buffercache.end())
		throw k3except(L"k3trans::k3trans() try to create secont transaction with this name = " + m_name);
}

k3trans::~k3trans()
{
	_ASSERTE(m_inProgress == false);

	if (m_transact != INVALID_HANDLE_VALUE)
	{
#ifdef ACID
		if (!CloseHandle(m_transact))
		{
			throw k3except(L"Could not close handle of transaction (error %d)", (int)GetLastError());
		}
#endif
	}
}

k3trans	*k3trans::getTransaction(wstring name)
{
	_ASSERT(0);
	return NULL;
}

void	k3trans::setCachesize(int sz)
// KBytes
{
	if(sz > 0)
		m_cachesize = sz;
}

void	k3trans::start(k3trans_isol is)
{
    _ASSERT(m_transact == INVALID_HANDLE_VALUE);
    _ASSERTE(m_inProgress == false);

    if (m_transact == INVALID_HANDLE_VALUE)
	{
		m_isolation = is;
        _ASSERTE(is == readcommited);

#ifdef ACID
		m_transact = CreateTransaction(0, 0, 0, 0, 0, NULL,(LPWSTR)m_name.c_str()); // !! 100ms is too small, set NULL
		if (m_transact == INVALID_HANDLE_VALUE)
		{

			throw k3except(L"k3trans::start() Could not create transaction (error %d)", (int)GetLastError());
			//printf("Could not create transaction (error %d)\n", GetLastError());
		}
#endif

	}

	m_inProgress = true;
}

void	k3trans::commit()
{
    _ASSERTE(m_inProgress);

	for(list<k3entity*>::iterator it_ent = m_ents.begin(); it_ent!= m_ents.end(); it_ent++)
	{
	    (*it_ent)->flush();
	}

#ifdef ACID
	_ASSERT(m_transact != INVALID_HANDLE_VALUE);

	if (m_transact != INVALID_HANDLE_VALUE)
	{
		if (!CommitTransaction(m_transact))
		{
			throw k3except(L"k3trans::commit() Could not commit transaction (error %d)", (int)GetLastError());
		}
		m_transact = INVALID_HANDLE_VALUE;
	}
#endif

	m_inProgress = false;

}

void	k3trans::rollback()
{
    _ASSERTE(m_inProgress);

	for(list<k3entity*>::iterator it_ent = m_ents.begin(); it_ent!= m_ents.end(); it_ent++)
	{
	    (*it_ent)->discard();
	}

	_ASSERT(m_transact != INVALID_HANDLE_VALUE);

	if (m_transact != INVALID_HANDLE_VALUE)
	{
#ifdef ACID
		if (!RollbackTransaction(m_transact))
		{
			throw k3except(L"k3trans::rollback() Could not rollback transaction (error %d)", (int)GetLastError());
		}
		m_transact = INVALID_HANDLE_VALUE;
#endif
	}

    m_inProgress = false;
}


void    k3trans::registerEntity(k3entity* ent)
{
    m_ents.push_back(ent);
}


void    k3trans::deregisterEntity(k3entity* ent)
{
    m_ents.remove(ent);
}

