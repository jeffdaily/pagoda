#ifndef PACK_H_
#define PACK_H_

extern void partial_sum(int g_src, int g_dst, int excl);
extern void pack(int g_src, int g_dst, int *g_masks, int *g_masksums);
extern void unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result);
extern void enumerate(int g_src, void *start, void *inc);
extern void unpack1d(int g_src, int g_dst, int g_msk);

#endif // PACK_H_
