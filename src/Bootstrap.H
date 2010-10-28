#ifndef PAGODA_BOOTSTRAP_H_
#define PAGODA_BOOTSTRAP_H_

namespace pagoda
{

    void initialize(int *argc, char ***argv);
    void finalize();

    extern int me;  /**< rank of this processing element */
    extern int npe; /**< number of global processing elements */

};

#endif // PAGODA_BOOTSTRAP_H_
