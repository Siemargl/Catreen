/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  demo test - speed test transactional & nontransacted objects creation
 *
 * Modifications history:
 */

#include "stdafx.h"

using namespace std;
#include <iostream>
#include "k3dbms.h"

// ======================================================================================
class k3testobj : public k3obj
{
	static	int gen_id;
public:
	int m_a, m_b, m_c;

	k3testobj();
	virtual k3obj *create() const { return new k3testobj(); }
	virtual wstring getPrimaryKeyName() const { return L"PRIMARY"; }
	k3testobj&	populate();
};

int k3testobj::gen_id;

k3testobj::k3testobj()
	: k3obj()
{
	d.m_size = sizeof *this - ((BYTE*)&d - (BYTE*)this);
}

k3testobj& k3testobj::populate()
{
	CoCreateGuid(&d.m_uid);
	d.m_prikey = ++gen_id;
	m_a = rand(); m_b = rand(); m_c = rand();
	return *this;
}

// ======================================================================================

void fnTestCreation(LPVOID lpParameter)
{

    if (!lpParameter) return;

	int	objnumber = *(int*)lpParameter;

	k3trans		trans1(L"transact1");
//	{  // when entity closes on destroy object, in saves cache

		k3entity	ent_objs(&trans1, L"Objs", k3testobj());
		ent_objs.drop();

	    startGlobalTimer();

		trans1.start();
		// create test set
		for (int i = 0; i < objnumber; i++)
		{
			k3testobj obj;
			obj.populate();
			ent_objs.save(obj);
			if (i % 10000 == 0) 
				cout << "Saved obj N: " << i << endl;
		}
		cout << endl << "Statistics creation:" << endl;
		cout << ent_objs.m_disk->m_statistics.print() << endl;
//	}
	trans1.commit();

    printGlobalTimer();
}

/*
int foreachfuncTestcnt = 0;
bool foreachfuncTest(k3obj* obj, void*)
// return true = break
{
    if (obj->d.m_prikey < 20)
		cout << "Have read obj UID: " << obj->d.m_prikey.key[0] << endl;
        //printf ("Have read obj UID: %I64i\n", (UID)*obj);


	if (++foreachfuncTestcnt % 15000) return false;

	cout << "Have read obj UID: " << obj->d.m_prikey.key[0] << endl;
	return false;
}
*/
bool foreachfuncTestRange(k3obj* obj, void*)
// return true = break
{
	cout << "Have read obj UID: " << obj->d.m_prikey.key[0] << endl;

	return false;
}

bool foreachfuncCount(k3obj* obj, void* count)
// return true = break
{
    (*(int*)count)++;

	return false;
}


void fnTestFullscan(LPVOID lpParameter)
{
    if (!lpParameter) return;

    startGlobalTimer();
	int	testcase = *(int*)lpParameter; //  1 - fullscan, 10000 - last 100


	k3trans		trans1(L"transact1");
//	{  // when entity closes on destroy object, in saves cache
		k3entity	ent_objs(&trans1, L"objs", k3testobj());

		trans1.start();
		// read all
		//ent_objs.forEach(foreachfuncTest, 0);
		if (testcase == 1)
		{
            int count = 0;
            ent_objs.forEach(foreachfuncCount, &count);
            cout << endl << "Count: " << count << endl;
		}
        else
            ent_objs.forEachKeyRange(testcase - 100, K3KEY(), foreachfuncTestRange, 0);

		cout << endl << "Statistics read:" << endl;
		cout << ent_objs.m_disk->m_statistics.print() << endl;
//	}
	trans1.commit(); // must also flush dirty blocks


    printGlobalTimer();
}
