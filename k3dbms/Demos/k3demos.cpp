/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  demo test - main() caller function
 *
 * Modifications history:
 */

#include "stdafx.h"

using namespace std;
#include <iostream>
#include "k3dbms.h"
#pragma warning(disable : 4996)

struct _timeb timebuffer1;

void    startGlobalTimer()
{
    _ftime(&timebuffer1);
}

void    printGlobalTimer()
{
    struct _timeb timebuffer2;
    _ftime(&timebuffer2);

    time_t time1, time2, difft;
    int millitm1, millitm2, diffm;

    time1 = timebuffer1.time;
    millitm1 = timebuffer1.millitm;
	time2 = timebuffer2.time;
	millitm2 = timebuffer2.millitm;

	difft = time2 - time1;
	diffm = millitm2 - millitm1;
	if (diffm < 0)
	{
		diffm += 1000;
		difft -= 1;
	}

	cout << endl << "Program takes " << difft << "." << diffm << " seconds" << endl;
}

// in k3demospeed.cpp
void	fnTestCreation(LPVOID lpParameter);
void	fnTestFullscan(LPVOID lpParameter);

// in k3demomulti.cpp
void	fnTestCacheSimple(LPVOID lpParameter);
void	fnTestCacheMulti();

// in k3demoindex.cpp
void	fnTestIndex(LPVOID lpParameter);
void	fnTestOLAP();

// in k3demoACID.cpp
void	fnTestACID();


int _tmain(int argc, _TCHAR* argv[])
{
#ifdef ACID
	cout << "WARN: ACID Requires Vista kernel functionality, ex.Vista, W7, WinServer 2008" << endl << endl;
#endif

	int     numobjects = 1000000, testnum = 0;
	if (testnum == 0)  // will entered in cmdline
	{
		if (argc == 2)
		{
			testnum = _wtoi(argv[1]);
		}
	}

	cout << "===============================================================================" << endl;
	cout << "K3DBMS embedded database library" << endl;
	cout << "Tool for test functionality, integrity check and demonstrations" << endl;
	if (testnum == 0)
	{
		cout << "Usage: k3demos.exe <param>   where param = one of below" << endl;
		cout << "10, create a number of records (1M) Entity Objs. ifdef ACID- will be transacted" << endl;
		cout << "20, full scan data from test 10, print count(*)" << endl;
		cout << "30, full scan data from test 10, print last 100 object's UID" << endl;
		cout << "40, Integrity check. Entity Clerks, Create, Update all, random read, 2-way cache check" << endl;
		cout << "50, Integrity check in multithreading. Test Clerks as previous" << endl;
		cout << "60, Variable length objects & indexing check" << endl;
		cout << "70, Aggregates & their automatic renegerating. OLAP fn not yet ready =/" << endl;
		cout << "80, Transation isolation / ACID base test" << endl;
	}
	cout << "===============================================================================" << endl;


	cout << "K3 demo apllication started. Test case: " << testnum << endl << endl;

	int param = 0;
    switch (testnum)
    {
        case 10:
            fnTestCreation(&numobjects);  // create a number of records (1M) Entity Objs. ifdef ACID - will be transacted
            break;
        case 20:
            param = 1;
            fnTestFullscan(&param);      // scan full Entity Objs. Count(*)
            break;
        case 30:
            param = numobjects;
            fnTestFullscan(&param);      // scan last 100 Objs. Analog for MAX()
            break;
        case 40:
            fnTestCacheSimple(0);      // Integrity check. Entity Clerks, Create, Update all, random read, 2-entity cache check.
            break;
        case 50:
            fnTestCacheMulti();      // Integrity check in multithreading. Test Clerks as previous
            break;
        case 60:
            fnTestIndex(0);      // Variable length objects & indexing check
            break;
        case 70:
            fnTestOLAP();      // Aggregates & their automatic renegerating
            break;
        case 80:
            fnTestACID();      // Transation isolation / ACID base test
            break;

    }

	k3entity_cache::cleanStatics();  // if you wish 
//	_CrtDumpMemoryLeaks();  // after normal work - it shows only 2 unfreed blocks (static bufcachemap + ?)

	return 0;
}



/*
tr.start(readcommited);
ent1(tr, "select * from accounts where payment < 40");
ent11(tr, "select * from accounts where age < 18");
ent12 = ent1.difference(ent11);  // paym < 40 but age >= 18
ent2(tr, "select * from clercs where name like a%");

ent2.foreach(clerk&)
{
	ent6(tr, "select * from accounts where account.clerkid==", clerk.id);
	int sum = ent6.sum("amount");
}

tr.commitclose();
*/

