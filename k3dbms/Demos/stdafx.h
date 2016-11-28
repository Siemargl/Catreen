// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#pragma once

#ifdef _DEBUG
// detect memory leak
//#define _CRTDBG_MAP_ALLOC
#endif
#include <stdlib.h>
#include <crtdbg.h>


#include "targetver.h"

#include <stdio.h>
#include <tchar.h>
#include <windows.h>
//#include <guiddef.h>

// TODO: reference additional headers your program requires here

#include <assert.h>
#include <process.h>
#include <time.h>
#include <sys/timeb.h>

#include <string>
#include <memory>
#include <map>
#include <list>
#include <vector>


void    startGlobalTimer();
void    printGlobalTimer();
