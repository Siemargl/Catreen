/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  demo test - index & aggregates test
 *
 * Modifications history:
 */

#include "stdafx.h"

using namespace std;
#include <iostream>
#include "k3dbms.h"

// ======================================================================================
// variable length structure + two index
class k3worker : public k3obj
{
	static int gen_id;
public:
	int	m_sernum;
	int m_age;
	wstring         m_fio;  // var.len String
	vector<wstring> m_children;  // var.len Array, may contain only PODs or wstrings
    map<wstring, wstring>  m_address; // // var.len Object, may contain only PODs or wstrings
    // {streetaddress, city, postalcode}

	k3worker() { d.m_size = sizeof *this - ((BYTE*)&d - (BYTE*)this) + 200; } // basic reservation size
	k3worker(int age);
	k3worker(const k3worker&) { _ASSERT(0); };
	virtual k3obj *create() const { return new k3worker(); }
	virtual wstring getPrimaryKeyName() const { return L"UID"; }
	virtual	void    getKey(const wstring &keyname, K3KEY &got) const;
	virtual	void	deserialize(BYTE*, int sz);
	virtual	void	serialize(BYTE*, int &sz);
	virtual	int	    getsize();
	k3worker        &populate();
};

int k3worker::gen_id;

k3worker::k3worker(int age)
	: k3obj()
{
	m_age = age;
	populate();

	d.m_size = getsize();

}


wstring     randomName()
{
    wstring     choice[] = {L"Andrew", L"Vladislav", L"Mike", L"Maxim", L"Boris",
        L"Serguis", L"Piter", L"Antotny", L"Grigory", L"Dimitry", L"Igor", L"Egor", L"Zinoviy",
        L"Evgeniy", L"Nikolay", L"Oleg", L"Roman", L"Svyatoslav", L"Fedor" };

    int names = sizeof choice / sizeof choice[0];
    int idx = rand() * (names -1) / RAND_MAX;

    return choice[idx];
}

wstring     randomName2()
{
    wstring     choice[] = {L" Андреевич", L" Владиславович", L" Михайлович", L" Максимович", L" Борисович",
        L" Сергеевич", L" Петрович", L" Антонович", L" Григорьевич", L" Дмитриевич", L" Игоревич", L" Егорович",
        L" Зиновьевич", L" Евгеньевич", L" Николаевич", L" Олегович", L" Романович", L" Святославович",
        L" Федорович", L" Владимирович" };

    int names = sizeof choice / sizeof choice[0];
    int idx = rand() * (names -1) / RAND_MAX;

    return choice[idx];
}

wstring     randomName3()
{
    wstring     choice[] = {L" I", L" II", L" III", L" IV", L" V",
        L" VI", L" VII", L" VIII", L" IX", L" X", L" XI", L" XII",
        L" XIII", L" XIV", L" XV", L" XVI", L" XVII", L" XVIII",
        L" XIX", L" XX" };

    int names = sizeof choice / sizeof choice[0];
    int idx = rand() * (names -1) / RAND_MAX;

    return choice[idx];
}

k3worker & k3worker::populate()
{
	CoCreateGuid(&d.m_uid);
	d.m_prikey = d.m_uid;
	m_sernum = ++gen_id;
    // random name
    m_fio = randomName() + randomName2() + randomName3();

    // random children
    int n = rand() * 10 / RAND_MAX;
    for (int i = 0; i < n; i++)
        m_children.push_back(randomName() + randomName3());

    // static address
	m_address[L"streetaddress"] = L"ул.Краснознаменная, 34";
	m_address[L"city"] = L"Большой Владикавказ";
    m_address[L"postalcode"] = L"567911";

	return *this;
}

void   k3worker::getKey(const wstring &keyname, K3KEY &got) const
{
    if (keyname == L"UID") got = d.m_uid;
    else if (keyname == L"AGE") got = m_age;
    else if (keyname == L"FIO") got = m_fio;
    else if (keyname == L"SERNUM") got = m_sernum;
    else throw k3except(L"k3worker::getKey() Invalid key requested");
}


void	k3worker::serialize(BYTE* bin, int &maxsz)
{
    serializeOn = true;

    int newlen = 0; // size increments

	serializeValue(bin, maxsz, d, newlen);
	serializeValue(bin, maxsz, m_sernum, newlen);
	serializeValue(bin, maxsz, m_age, newlen);

	serializeValue(bin, maxsz, m_fio, newlen);
	serializeArray(bin, maxsz, m_children, newlen);
	serializeMap(bin, maxsz, m_address, newlen);

    maxsz = newlen;
}

void	k3worker::deserialize(BYTE* bin, int maxsz)
{
    int newlen = 0; // size increments

	deserializeValue(bin, maxsz, d, newlen);
	deserializeValue(bin, maxsz, m_sernum, newlen);
	deserializeValue(bin, maxsz, m_age, newlen);

	deserializeValue(bin, maxsz, m_fio, newlen);
	deserializeArray(bin, maxsz, m_children, newlen);
	deserializeMap(bin, maxsz, m_address, newlen);

}


int	    k3worker::getsize()
{
    serializeOn = false;

    int newlen = 0, maxsz = 1000000;
    BYTE* bin = NULL;

    // just a copy. when serializeOff, no writes, only size advances

	serializeValue(bin, maxsz, d, newlen);
	serializeValue(bin, maxsz, m_sernum, newlen);
	serializeValue(bin, maxsz, m_age, newlen);

	serializeValue(bin, maxsz, m_fio, newlen);
	serializeArray(bin, maxsz, m_children, newlen);
	serializeMap(bin, maxsz, m_address, newlen);

    serializeOn = true;
    return newlen;
}


bool printFunctor(k3obj *obj, void *)
{
	k3worker*	ow = dynamic_cast<k3worker*>(obj);
	char ansi_fio[100];
#pragma warning(disable : 4996)

	size_t count = wcstombs((char*)ansi_fio, ow->m_fio.c_str(), sizeof ansi_fio);
	_ASSERT(count != (size_t)-1); // error condition

	cout << ow->m_sernum << " : " << ansi_fio << endl;

// alternative - use wcout <<

	return false;
}

// ======================================================================================

void	fnTestIndex(LPVOID lpParameter)
{
	startGlobalTimer();

	int	objnumber = 1000;
	bool chkok = true;
	k3trans		trans(L"transaction");
	k3entity	workers(&trans, L"workers", k3worker());
	k3entityIndex i3age(workers, L"AGE");
	i3age.m_blocksize = 1000;  // index is very dense, 1000 = 10 keys per fileblock
	k3entityIndex i3fio(workers, L"FIO");
	k3entityIndex i3sernum(workers, L"SERNUM");
	workers.drop();
	trans.start();

	int aged[101];   // number of aged(i) = i
	memset(aged, 0, sizeof aged);
	k3objheader *poh = new k3objheader[objnumber];

	// create test set
	for (int i = 0; i < objnumber; i++)
	{
		int nage = rand() * 100 / RAND_MAX;
		k3worker o(nage);
		aged[nage] ++;

#ifdef _DEBUG
 // check hash stability
		K3KEY k = o.d.m_prikey;
		K3KEY k1, k2;
		workers.hash(k, k1);
		workers.hash(k1, k2);
		_ASSERTE(k1 == k2);
#endif		
		
		workers.save(o);

		poh[i] = o.d;

		if (i % 100 == 0)
            cout << "saved worker N: " << o.m_sernum << endl;
	}

	cout << endl << "Statistics creation:" << endl;
	cout << workers.m_disk->m_statistics.print() << endl;

#ifdef _DEBUG
	//check reload. Debuging index integrity
	for (int i = 0; i < objnumber; i++)
	{
		k3worker o;
		bool rc = workers.load(poh[i], o);
		if (!rc)
		{
			cout << "error load worker N: " << i << endl;
			rc = workers.load(poh[i], o);
		}
	}
#endif
	delete [] poh;



	//check testset (key == age)
	for (int i = 1; i < 101; i++)
	{
		k3worker o;
		UIDvector	ovec;
		ovec.clear();
		K3KEY	key(i);
		bool rc = workers.load(key, ovec, L"AGE");
		if (rc)
		{
            rc = workers.load(ovec[0], o);

			if(o.m_age != i)
			{
                cout << "Error age check worker N: " << o.m_sernum << " aged " << o.m_age << endl;
				chkok = false;
			}

			if(ovec.size() != aged[i])
			{
			    cout << "Error in count(*) when age = " << i << endl;
				chkok = false;
			}
		}
	}

	cout << endl << "Statistics TOTAL:" << endl;
	cout << workers.m_disk->m_statistics.print() << endl;


	if (chkok)
		printf ("test OK on: %i\n", objnumber);
	else
		printf ("test FAIL on: %i\n", objnumber);

	trans.commit();

	printGlobalTimer();

	cout << endl << "Workers from N150 to N186 sorted by number" << endl << endl;
	workers.forEachKeyRange(150, 186, printFunctor, 0, L"SERNUM");

	cout << endl << "Workers from 'E' to 'O' sorted alphabetically" << endl << endl;
	workers.forEachKeyRange(K3KEY(L"E"), K3KEY(L"Ozzzzzzzzzzzzzzzzzzzz"), printFunctor, 0, L"FIO");

}



void	fnTestOLAP()
{
	_ASSERT(0);
	startGlobalTimer();
	printGlobalTimer();
}
