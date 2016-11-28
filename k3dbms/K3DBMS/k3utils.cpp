/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Small helper objects - classes k3stat, k3obj, serialization templates
 *
 * Modifications history:
 */

#include "stdafx.h"
#include <mbctype.h>
#include <locale.h>
#include <errno.h>

using namespace std;
#include "k3dbms.h"

#pragma warning(disable : 4996)

//================================ realisation ==================================
K3KEY::K3KEY(const wstring &s)
{
    size_t  count;

	char	strReversed[17];

//	 printf("The locale is now set to %s.\n", setlocale(...));
	setlocale(LC_CTYPE, ".1251");

	count = wcstombs(strReversed, s.c_str(), sizeof strReversed - 1);
	_ASSERT(count != (size_t)-1); // error condition

	// to ascend keys by string order, need add leading spaces and reverse 
	strReversed[16] = 0;
	strncat(strReversed, "                 ", 16 - strlen(strReversed));
	strrev(strReversed);

/*	
	union
	{
		__int64 emukey[2];
		char	emuchar[16];
		SYSTEMTIME emudate;
	} var;
	strncpy(var.emuchar, strReversed, sizeof var.emuchar); // check

	SYSTEMTIME date;
	date.wYear = 1999;
	date.wMilliseconds = 100;
	var.emudate = date; // check
*/

	memcpy(key, strReversed, sizeof key);

#ifndef __DMC__
	errno_t err;
	_get_errno( &err );
#endif

	isNull = false;
};

K3KEY::K3KEY(const SYSTEMTIME &s) 
{ 
	SYSTEMTIME reversed; // change word-order

	reversed.wYear = s.wMilliseconds;
	reversed.wMonth = s.wSecond;
	reversed.wDayOfWeek = s.wMinute;
	reversed.wDay = s.wHour;
	reversed.wHour = s.wDay;
	reversed.wMinute = s.wDayOfWeek;
	reversed.wSecond = s.wMonth;
	reversed.wMilliseconds = s.wYear;
  
	memcpy(key, &reversed, sizeof key); 
	isNull = false; 
}



K3KEY::K3KEY(const UID &uid)
{
	_ASSERTE(sizeof (UID) == sizeof key);
	memcpy(key, &uid, sizeof key);

	isNull = false;
}


//================================ realisation ==================================
k3stat::k3stat()
{
	logical_reads = 0;
	block_changes = 0;
	physical_reads = 0;
	physical_writes = 0;
}

const k3stat&	k3stat::operator +=(const k3stat& tr)
{
	logical_reads += tr.logical_reads;
	block_changes += tr.block_changes;
	physical_reads += tr.physical_reads;
	physical_writes += tr.physical_writes;
	return *this;
}

string k3stat::print()
{
    char  buf[200];

	sprintf(buf, "logical reads: %i\nblock changes: %i\nphysical reads: %i\nphysical writes: %i\n",
		logical_reads, block_changes, physical_reads, physical_writes);

	return buf;
}



//================================ realisation ==================================

k3obj::k3obj()
{
//	m_data = reinterpret_cast<BYTE*>(&d);  /// WTF on 1st call !

	memset(&d, 0, sizeof d);  //  zero struct gaps

	d.m_size = sizeof d;
}

k3obj::~k3obj() {}

/*
k3obj::k3obj(const k3obj& obj)
{
	m_data = reinterpret_cast<BYTE*>(&d);
	d = obj.d;
}
*/

k3obj& k3obj::operator =(const k3obj& obj)
{
	if (&obj == this) return *this;

	_ASSERT(d.m_size == obj.d.m_size);

	memcpy(&d, &obj.d, d.m_size);
	return *this;
}


bool k3obj::operator < (const k3obj& obj) const
{
	return d.m_prikey.key < obj.d.m_prikey.key;
}

k3obj::operator UID()
{
	return d.m_uid;
}

void	k3obj::deserialize(BYTE* bin, int len)
// call from reader (disk->obj)
// must check schema version
{
	_ASSERT (d.m_size == len);
	_ASSERT(bin);
	memcpy(&d, bin, d.m_size);
}

void	k3obj::serialize(BYTE* bin, int &len)
// call from writer (obj->disk)
{
	_ASSERT(bin);
	_ASSERT(len);

	memcpy(bin, &d, d.m_size);
	len = d.m_size;
}


int	k3obj::getsize()
{
	return d.m_size;
}


