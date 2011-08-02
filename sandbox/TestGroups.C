#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif

#include <stdint.h>

// C++ includes, std and otherwise
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <mpi.h>
#include <ga.h>

#include "pagoda.H"

int main(int argc, char **argv)
{
    pagoda::initialize(&argc, &argv);

    ProcessGroup group1(pagoda::me);
    ProcessGroup group2(0);
    ProcessGroup group3(pagoda::me%2);
    ProcessGroup group4(pagoda::me>=(pagoda::npe/2));

    pagoda::println_sync(pagoda::to_string(group1.get_id()) + " "
            + pagoda::vec_to_string(group1.get_ranks()));
    pagoda::println_sync(pagoda::to_string(group2.get_id()) + " "
            + pagoda::vec_to_string(group2.get_ranks()));
    pagoda::println_sync(pagoda::to_string(group3.get_id()) + " "
            + pagoda::vec_to_string(group3.get_ranks()));
    pagoda::println_sync(pagoda::to_string(group4.get_id()) + " "
            + pagoda::vec_to_string(group4.get_ranks()));

    pagoda::finalize();
    return EXIT_SUCCESS;
}
