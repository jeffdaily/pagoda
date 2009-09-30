#include "Debug.H"
#include "Timing.H"

int __gcrm_subsetter_get_precision()
{
    TIMING("__gcrm_subsetter_get_precision()");
    int precision = 1;
    int nproc = NPROC;
    while (nproc > 10) {
        ++precision;
        nproc /= 10;
    }
    return precision;
}
