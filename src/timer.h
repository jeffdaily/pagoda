#ifndef PAGODA_TIMER_H_
#define PAGODA_TIMER_H_

typedef unsigned long long utimer_t;

#if defined(__i386__) || defined(__x86_64__) || defined(__powerpc__)
#   if defined(__i386__)
static __inline__ utimer_t rdtsc(void)
{
    utimer_t x;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
    return x;
}
#   elif defined(__x86_64__)
static __inline__ utimer_t rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (utimer_t)lo)|( ((utimer_t)hi)<<32 );
}
#   elif defined(__powerpc__)
static __inline__ utimer_t rdtsc(void)
{
    utimer_t result=0;
    unsigned long int upper, lower,tmp;
    __asm__ volatile(
            "0:                  \n"
            "\tmftbu   %0        \n"
            "\tmftb    %1        \n"
            "\tmftbu   %2        \n"
            "\tcmpw    %2,%0     \n"
            "\tbne     0b        \n"
            : "=r"(upper),"=r"(lower),"=r"(tmp)
            );
    result = upper;
    result = result<<32;
    result = result|lower;

    return(result);
}
#   endif
#elif HAVE_WINDOWS_H
#   include <windows.h>
#elif HAVE_SYS_TIME_H
#   include <sys/time.h>
#endif

static utimer_t timer_start()
{
#if defined(__i386__) || defined(__x86_64__) || defined(__powerpc__)
    return rdtsc();
#elif HAVE_WINDOWS_H
    LARGE_INTEGER timer;
    QueryPerformanceCounter(&timer);
    return timer.QuadPart * 1000 / frequency.QuadPart;
#elif HAVE_SYS_TIME_H
    struct timeval timer;
    (void)gettimeofday(&timer, NULL);
    return timer.tv_sec*1000000 + timer.tv_usec;
#else
    return 0;
#endif
}

static utimer_t timer_end(utimer_t begin)
{
    return timer_start()-begin;
}

static void timer_init()
{
#if HAVE_WINDOWS_H
    QueryPerformanceFrequency(&frequency);
#endif
}

static const char* timer_name()
{
#if defined(__i386__) || defined(__x86_64__) || defined(__powerpc__)
    return "rdtsc";
#elif HAVE_WINDOWS_H
    return "windows QueryPerformanceCounter";
#elif HAVE_SYS_TIME_H
    return "gettimeofday";
#else
    return "no timers";
#endif
}

#endif /* PAGODA_TIMER_H_ */
