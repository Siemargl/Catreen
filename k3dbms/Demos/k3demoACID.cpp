/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  demo test - ACID test in multithreading
 *
 * Modifications history:
 */

#include "stdafx.h"

using namespace std;
#include <iostream>
#include "k3dbms.h"

#define OBJNUM 100000
HANDLE ghEvents[2];


// ======================================================================================
class k3acidobj : public k3obj
{
	static	int gen_id;
public:
	int m_a, m_b, m_c;

	k3acidobj();
	virtual k3obj *create() const { return new k3acidobj(); }
	virtual wstring getPrimaryKeyName() const { return L"PRIMARY"; }
	k3acidobj&	populate();
};

int k3acidobj::gen_id;

k3acidobj::k3acidobj()
	: k3obj()
{
	d.m_size = sizeof *this - ((BYTE*)&d - (BYTE*)this);
}

k3acidobj& k3acidobj::populate()
{
	CoCreateGuid(&d.m_uid);
	d.m_prikey = ++gen_id;
	do
	{
		m_a = rand(); 
	} while(m_a == 1000 || m_a == 2000); // exclude test values
	m_b = rand(); 
	m_c = rand();
	return *this;
}

// ======================================================================================
struct	fnCountIfFunctorData
{
	int		value;
	int		count;
};

bool	fnCountIfFunctor(k3obj* objp, void* param)
{
	k3acidobj* obj = dynamic_cast<k3acidobj*>(objp);
	fnCountIfFunctorData* pdata =(fnCountIfFunctorData*)param;

	if (obj->m_a == pdata->value)
		pdata->count++;

	return false;

}


//Lost update. No chance in isolation > dirty read
//Dirty Read check also
void fnTestLostUpdateTh1(LPVOID lpParameter)
// Thread1
{
	bool failed = false;  // test result

	int count1k = 0;
	int cnterr = 0;

	k3acidobj obj;
	k3trans		trans(L"transaction th1");
	k3entity	e3objects(&trans, L"ACID", obj);
	trans.start();

	for (int i = 1; i < 10000; i++)
	{
		__int64		intkey = (__int64)rand() * (OBJNUM-1) / RAND_MAX + 1;
		_ASSERTE(intkey > 0);


		bool rc = e3objects.loadUni(intkey, obj);
		if (!rc)
		{
			cout << "Error. Not found object N: " << intkey << endl;
			failed = true;
		}
		if (obj.m_a == 2000)  /// overwrited by second thread
		{
			cout << "Error. Dirty Read detected in N: " << intkey << endl;
			failed = true;
		}

		if (obj.m_a != 1000)
		{
			try
			{
				obj.m_a = 1000;  // flag th1
				e3objects.save(obj);
				count1k++;
			}
			catch(k3except &xc)
			{
				wcout << "Thread 1 error: " << xc.s_error << endl;
				cnterr++;
			}

		}
	}
	trans.commit();

	fnCountIfFunctorData data = {1000, 0};
	e3objects.forEach(fnCountIfFunctor, &data);

	if (data.count != count1k)
	{
		cout << "Error 'Lost Update' detected" << endl;
		failed = true;
	}

	if (failed)
		cout << "ACID test 'Lost update & 'Dirty Read' FAIL" << endl  << endl;
	else
		cout << "ACID test 'Lost update' & 'Dirty Read' PASS" << endl << endl;

    if ( !SetEvent(ghEvents[0]) ) 
    {
        printf("SetEvent failed (%d)\n", GetLastError());
    }
}


void fnTestLostUpdateTh2(LPVOID lpParameter)
// Thread2
{
	bool failed = false;  // test result

	k3acidobj obj;
	k3trans		trans(L"transaction th2");
	k3entity	e3objects(&trans, L"ACID", obj);
	trans.start();

	for (int i = 1; i < 10000; i++)
	{
		__int64		intkey = (__int64)rand() * (OBJNUM-1) / RAND_MAX + 1;

		_ASSERTE(intkey > 0);

		bool rc = e3objects.loadUni(intkey, obj);
		if (!rc)
		{
			cout << "Error in th2. Not found object N: " << intkey << endl;
			failed = true;
		}
		try
		{
			obj.m_a = 2000;  // flag th2
			e3objects.save(obj);
		}
		catch(k3except &xc)
		{
			wcout << "Thread 2 error: " << xc.s_error << endl;
			cout << "Error 32 = SHARING VIOLATION, because of transactional locking" << endl;
		}

	}
	trans.rollback(); // sure, no value 2000

    if ( !SetEvent(ghEvents[1]) ) 
    {
        printf("SetEvent failed (%d)\n", GetLastError());
    }
}


void fnTestRepeatableRead(LPVOID lpParameter)
// Repeatable read must fail when isolation == RC, DR
{
	bool failed = false;  // test result

	int count1k = 0;

	k3acidobj obj;
	k3trans		trans(L"transaction1");
	k3trans		trans2(L"transaction2");
	k3entity	e3objects(&trans, L"ACID", obj);
	k3entity	e3objects2(&trans2, L"ACID", obj);
	trans.start();
	trans2.start();

	// how many obj.m_a == 1000 in SECOND transaction
	fnCountIfFunctorData data = {1000, 0};
	e3objects2.forEach(fnCountIfFunctor, &data);
	count1k = data.count;

	cout << "ACID Test 'Repeatable Read'. Count before = " << count1k << endl;

	//change randoms in FIRST transaction
	for (int i = 1; i < 10000; i++)
	{
		__int64		intkey = (__int64)rand() * (OBJNUM-1) / RAND_MAX + 1;

		bool rc = e3objects.loadUni(intkey, obj);
		if (!rc)
		{
			cout << "Error. Not found object N: " << intkey << endl;
			failed = true;
		}
		if (obj.m_a == 1000)
		{
			obj.m_a++;  
			e3objects.save(obj);
		}
	}
	trans.commit();

	// how many obj.m_a == 1000 in SECOND transaction, after FIRST committed
	data.count = 0;
	e3objects2.forEach(fnCountIfFunctor, &data);

	cout << "ACID Test 'Repeatable Read'. Count after = " << data.count << endl;

	if (data.count != count1k)
	{
		cout << "Error 'UN Repeatable Read' detected" << endl;
		failed = true;
	}

	if (failed)
		cout << endl << "ACID test 'Repeatable Read' FAIL" << endl;
	else
		cout << endl << "ACID test 'Repeatable Read' PASS" << endl;

	// trans2 must rollback automatic, ASSERTed, but continued runs right
}


void	createTestData()
{
	k3trans		trans1(L"transact1");
	k3entity	ent_objs(&trans1, L"ACID", k3acidobj());
	ent_objs.drop();

	trans1.start();
	// create test set
	for (int i = 0; i < OBJNUM; i++)
	{
		k3acidobj obj;
		obj.populate();
		ent_objs.save(obj);
		if (i % 10000 == 0) 
			cout << "Saved obj N: " << i << endl;
	}
	trans1.commit();

	cout << endl << "ACID Test data created" << endl << endl;
}



void	fnTestACID()
{
#ifndef ACID
	cout << "ACID not #defined, unpredictable result('Dirty Read')" << endl;
#endif

	createTestData();

	/// ... do threadwork
	int	i;
    for (i = 0; i < 2; i++) 
    { 
        ghEvents[i] = CreateEvent( 
            NULL,   // default security attributes
            FALSE,  // auto-reset event object
            FALSE,  // initial state is nonsignaled
            NULL);  // unnamed object

        if (ghEvents[i] == NULL) 
        { 
            printf("CreateEvent error: %d\n", GetLastError() ); 
            ExitProcess(0); 
        } 
    }

	_beginthread( fnTestLostUpdateTh2, 000, NULL );

	Sleep( 1000L );
	_beginthread( fnTestLostUpdateTh1, 000, NULL );

    DWORD dwEvent = WaitForMultipleObjects( 
        2,           // number of objects in array
        ghEvents,     // array of objects
        TRUE,       // wait for all object
        INFINITE);
    for (i = 0; i < 2; i++) 
        CloseHandle(ghEvents[i]);  

	fnTestRepeatableRead(0);
//	while (nthr < 3) Sleep(100L);

	//fnTestCacheSimpleKey(0);
	//fnTestCacheSimple(0);

	/// ... end all threads
}


