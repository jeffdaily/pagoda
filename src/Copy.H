#include "DataType.H"

namespace pagoda {
    template <class T, class InputIterator1, class InputIterator2>
    void copy_cast(InputIterator1 first, InputIterator1 last,
            InputIterator2 result)
    {
        while (first!=last) {
            *result++ = static_cast<T>(*first++);
        }
    }

    void copy(DataType src_dt, void *src,
              DataType dst_dt, void *dst, int64_t n);
};
