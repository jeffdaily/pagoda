#ifndef PAGODA_BOOTSTRAP_H_
#define PAGODA_BOOTSTRAP_H_

#include <string>

using std::string;

namespace pagoda
{

    void initialize(int *argc, char ***argv);
    void finalize();
    void abort(const string &message);
    void abort(const string &message, int errorcode);
    int64_t num_nodes();
    int64_t nodeid();

    extern int me;  /**< rank of this processing element */
    extern int npe; /**< number of global processing elements */
    extern MPI_Comm COMM_WORLD; /**< MPI_Comm_dup of MPI_COMM_WORLD */
};

#endif // PAGODA_BOOTSTRAP_H_
