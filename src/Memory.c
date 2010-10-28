#if HAVE_CONFIG_H
#   include <config.h>
#endif

#if HAVE_GA
#   include <ga.h>
#endif

#include <stdlib.h>

void* pagoda_malloc(size_t bytes, int align, char *name)
{
    return malloc(bytes);
}

void pagoda_free(void *ptr)
{
    free(ptr);
}

void pagoda_register_stack_memory()
{
#if HAVE_GA
    GA_Register_stack_memory(pagoda_malloc,pagoda_free);
#endif
}
