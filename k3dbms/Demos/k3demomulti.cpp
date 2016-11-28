/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  demo test - creation & multithreading
 *
 * Modifications history:
 */

#include "stdafx.h"

using namespace std;
#include <iostream>
#include "k3dbms.h"

LPCRITICAL_SECTION	hSection;

// ======================================================================================
class k3clerk : public k3obj
{
	static long gen_id;  // generator
public:
	long int	m_sernum;
	int m_age;
	wchar_t m_fio[50];
	k3clerk() { d.m_size = sizeof *this - ((BYTE*)&d - (BYTE*)this); }
	k3clerk(int age);
	virtual k3obj *create() const { return new k3clerk(); }
	virtual wstring getPrimaryKeyName() const { return L"PRIMARY"; } // sernum
};

long k3clerk::gen_id;

k3clerk::k3clerk(int age)
	: k3obj()
{
	// d.m_size += sizeof m_age + sizeof m_fio;
	// !error - some data may lost due to struct alignment

	d.m_size = sizeof *this - ((BYTE*)&d - (BYTE*)this);
	CoCreateGuid(&d.m_uid);  // because of this - may be many object with single sernum

	if (hSection)
			EnterCriticalSection(hSection);  // else sernum doubles

	m_sernum = ++gen_id;
	
	if (hSection)
			LeaveCriticalSection(hSection);

	d.m_prikey = m_sernum;
	m_age = age;
	wsprintf(m_fio, L"N %I32i Клерк Вася Пупкин, ID%I32i, %i лет", m_sernum, d.m_uid.Data1, m_age);
}

// ======================================================================================


static int		nthr;
void fnTestCacheSimple(LPVOID lpParameter)
{
	int	objnumber = 100000;
	bool chkok = true;

	try
	{
		k3entity	clerks(NULL, L"clerks", k3clerk());  // nontransacted operation

		if (lpParameter == NULL) // test single
		{
			clerks.drop();
			lpParameter = L"_single";
		}

		k3trans		trans2(L"transact1" + wstring((LPCTSTR)lpParameter));
		k3entity	clerks2(&trans2, L"clerks", k3clerk()); // transaction
		trans2.start();

		// create test set
		for (int i = 0; i < objnumber; i++)
		{
			k3clerk o(0);  o.m_age = o.m_sernum % 100 + 1;
			clerks.save(o);
			if (i % 10000 == 0)
				cout << "saved clerk N: " << o.m_sernum << endl;
		}

		cout << endl << "Statistics creation:" << endl;
		cout << clerks.m_disk->m_statistics.print() << endl;

		if (wstring((LPCTSTR)lpParameter) == L"_single")
		// touch all through other entity object. In multithread sharing conflicts
		for (int i = 0; i < objnumber; i++)
		{
			k3clerk o;
			bool rc = clerks2.loadUni(i+1, o);
			if (!rc)
			{
				cout << "trans1 clerk N: " << o.m_sernum << " not found" << endl;
				chkok = false;
			}
			else
			clerks2.save(o);
		}

		cout << endl << "Statistics touching:" << endl;
		cout << clerks.m_disk->m_statistics.print() << endl;

		//check testset
		for (int i = 0; i < objnumber; i++)
		{
			k3clerk o;
			bool rc = clerks.loadUni(i + 1, o);

			if (rc)
			{
				if(o.m_age != o.m_sernum % 100 + 1)
				{
					cout << "Error age check clerk N: " << o.m_sernum << " aged " << o.m_age << endl;
					chkok = false;
				}
				if (i % 10000 == 0)
					cout << "Loaded clerk N: " << o.m_sernum << endl;
			}
			else
			{
				cout << "Error. Not found clerk N: " << i+1 << endl;
				chkok = false;
			}

		}

		cout << endl << "Statistics checkread:" << endl;
		cout << clerks.m_disk->m_statistics.print() << endl;


		//random read testset
		for (int i = 0; i < objnumber; i++)
		{
			k3clerk o;
			K3KEY rndkey(rand() * 7000 /*objnumber*/ / RAND_MAX + 1);
			if (K3KEY(objnumber) < rndkey) rndkey = objnumber;

			bool rc = clerks.loadUni(rndkey, o);

			if (rc)
			{
				if (i % 10000 == 0)
					cout << "Loaded random clerk N: " << o.m_sernum << endl;
			}
			else
			{
				cout << "Error. Not found clerk N: " << i+1 << endl;
				chkok = false;
			}

		}


		cout << endl << "Statistics TOTAL:" << endl;
		cout << clerks.m_disk->m_statistics.print() << endl;

		if (chkok)
			printf ("test OK on: %i\n", objnumber);
		else
			printf ("test FAIL on: %i\n", objnumber);

		trans2.commit();
	}
	catch(k3except &xc)  // if not catched, thread would run crazy (
	{
		cout << "Thread ended abnormally." << endl;
		wcout << xc.s_error << endl;
	}
	nthr ++;
}



void	fnTestCacheMulti()
{
	hSection = new CRITICAL_SECTION;
	InitializeCriticalSection(hSection);

	k3entity	clerks(NULL, L"clerks", k3clerk());  
	clerks.drop();  // else got non-unique id

	/// ... do threadwork
	startGlobalTimer();

	const wchar_t	*begin = L" 0";

	_beginthread( fnTestCacheSimple, 000, (LPVOID)begin );
	Sleep( 1000L );
	begin = L" 1";
	_beginthread( fnTestCacheSimple, 000, (LPVOID)begin );
	Sleep( 1000L );
	begin = L" 2";
	_beginthread( fnTestCacheSimple, 000, (LPVOID)begin );

	while (nthr < 3) Sleep(100L);

	printGlobalTimer();
	/// ... end all threads
	DeleteCriticalSection(hSection);
}


