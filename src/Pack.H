#ifndef PACK_H_
#define PACK_H_

#include <stdint.h>

#include <vector>

using std::vector;

class Array;


/**
 * Holder of various miscellanous functionality.
 */
namespace pagoda
{
    void partial_sum(const Array *g_src, Array *g_dst, bool excl);
    void pack(const Array *g_src, Array *g_dst, const vector<Mask*> &g_masks);
    void pack(const Array *g_src, Array *g_dst,
              const vector<Array*> &g_masks,
              const vector<Array*> &g_masksums);
    void unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result);
    void unravel64(int64_t x,
                   const vector<int64_t> &dims,
                   vector<int64_t> &result);
    void enumerate(Array *src, void *start, void *inc);
    void unpack1d(const Array *g_src, Array *g_dst, Array *g_msk);
    void renumber(Array *array, const Array *index);
};

#endif // PACK_H_
