#ifndef PACK_H_
#define PACK_H_

class Array;

/**
 * Holder of various miscellanous functionality.
 */
namespace pagoda
{
    void partial_sum(int g_src, int g_dst, int excl);
    void pack(int g_src, int g_dst, int *g_masks, int *g_masksums);
    void unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result);
    void enumerate(int g_src, void *start, void *inc);
    void enumerate(Array *src, void *start, void *inc);
    void unpack1d(int g_src, int g_dst, int g_msk);
    void unpack1d(Array *g_src, Array *g_dst, Array *g_msk);
};

#endif // PACK_H_
