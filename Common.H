#ifndef COMMON_H_
#define COMMON_H_

#include <cmath> // for M_PI

#include <ga.h>

#define ME GA_Nodeid()
#define NPROC GA_Nnodes()
#define MAX_NAME 80
#define RAD_PER_DEG (M_PI/180.0)
#define DEG_PER_RAD (180.0/M_PI)

#endif // COMMON_H_
