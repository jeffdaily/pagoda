#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Copy.H"
#include "Error.H"

void pagoda::copy(DataType src_dt, void *src,
                  DataType type, void *dst, int64_t n)
{
    if (DataType::CHAR == src_dt) {
        char *asrc = static_cast<char*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::SHORT == src_dt) {
        short *asrc = static_cast<short*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::INT == src_dt) {
        int *asrc = static_cast<int*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::LONG == src_dt) {
        long *asrc = static_cast<long*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::LONGLONG == src_dt) {
        long long *asrc = static_cast<long long*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::FLOAT == src_dt) {
        float *asrc = static_cast<float*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::DOUBLE == src_dt) {
        double *asrc = static_cast<double*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::LONGDOUBLE == src_dt) {
        long double *asrc = static_cast<long double*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::UCHAR == src_dt) {
        unsigned char *asrc = static_cast<unsigned char*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::USHORT == src_dt) {
        unsigned short *asrc = static_cast<unsigned short*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::UINT == src_dt) {
        unsigned int *asrc = static_cast<unsigned int*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::ULONG == src_dt) {
        unsigned long *asrc = static_cast<unsigned long*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::ULONGLONG == src_dt) {
        unsigned long long *asrc = static_cast<unsigned long long*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    if (DataType::SCHAR == src_dt) {
        signed char *asrc = static_cast<signed char*>(src);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
        T *adst = static_cast<T*>(dst); \
        copy_cast<T>(asrc,asrc+n,adst); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else
    {
        EXCEPT(DataTypeException, "DataType not handled", src_dt);
    }
}
