========================================================================
/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * K3DBMS is a ligthweight object database library for Windows Vista Kernels
 * supports such functionality as:
 * - thread-safe caching;
 * - transactions (NTFS TxF-based);
 * - store complex objects as standartized in JSON;
 * - automatic base OLAP structures;
 * - no other libraries used, just JSON
 *
 * Modifications history:
 *
 * Version 1.0. Base functions,  no OLAP-fn, no JSON-translation sample
 */
========================================================================

Files:

K3DBMS subcatalog contains library files (target - static k3dbms.lib):

K3DBMS.vcproj
    This is the main project file for VC++ projects generated using an Application Wizard.
    It contains information about the version of Visual C++ that generated the file, and
    information about the platforms, configurations, and project features selected with the
    Application Wizard.

CCRC32.cpp
	CRC32 count routines
k3bufblock.cpp
    class k3bufferblock. File block, used as sole entity in cache and file store operations
k3bufcache.cpp
	class k3entity_cache. In memory buffer cache, use write-once algorithm
k3disk.cpp
	class k3diskgroup. File operations, can be used for partitioning and disk-level optimization
k3entity.cpp
	class k3entity. Object storage - user interface
k3index.cpp
	classes k3entityDepends, k3entityIndex. Automatically recounted "children"-entities
k3trans.cpp
	class k3trans. Transaction wrapper
k3utils.cpp
	Helper classes K3KEY - indexing hash-key, k3stat - diskcache statistics, k3obj - base class 
	for user-defined stored objects

Demos subcatalog contains test files and samples using all edges of functionality:
	
k3demos.cpp
	Main file. See description in main.
Other
	See descriptions in titles of each file   

/////////////////////////////////////////////////////////////////////////////
TODO:

==Primaries==
1. JSON
5. file distribution by UID very weird - too many files

==Tests && docs==
0. speed - create 1object with 3m params array
1. Complex demo. airplanes
2. OLAP demo
21. functors using  max(), min(), avg(),....
3. K3obj to JSON serialize
31.	Metadata - entity name, dependend names and indexes for auto-open. Data\entity_name.json
32. JSON expimp demo
33. Check Metadata when dropping entities. 
4. TPC demo. TPC-? (object TPC)

==Debug base funcs==
5. check resource leak on except // part.done

==Optimize && functionality==
0. NULL type
01. Optimize saving unchanged obj
02. Rewrite using STL algorithms
1. Browse binary by JSON-file object structure (may change later to BSON)
2. k3entityDepend operations (algebraic set)
3. Remove/optimize giant lock on cache
31 Disk parallelizm
4. High level of transisolation
5. move hashFname to disk:: with block.load(,....offset)

Changes
Removed all wcscpy_s & wcscat_s, because of nonportability (WATCOM, DMC)

/////////////////////////////////////////////////////////////////////////////
