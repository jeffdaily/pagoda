#ifndef GAX_H_
#define GAX_H_

namespace gax
{

    void partial_sum(int g_src, int g_dst, int excl);
    void pack(int g_src, int g_dst, int *g_masks, int *g_masksums);
    void unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result);
    void enumerate(int g_src, void *start, void *inc);
    void unpack1d(int g_src, int g_dst, int g_msk);

};

#endif /* GAX_H_ */
