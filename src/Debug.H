#ifndef DEBUG_H_
#define DEBUG_H_

#include "Print.H"

#define TAG_DEBUG 33284

#if defined(DEBUG) || defined(TRACE)
#   define DEBUG_ZERO pagoda::print_zero
#   define DEBUG_SYNC pagoda::print_sync
#else
#   define DEBUG_ZERO pagoda::print_zero_dummy
#   define DEBUG_SYNC pagoda::print_sync_dummy
#endif

#endif // DEBUG_H_
