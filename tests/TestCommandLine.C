/** Perform various block-sized pnetcdf reads. */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>

#include "Bootstrap.H"
#include "Debug.H"
#include "SubsetterCommands.H"

using std::cerr;
using std::cout;
using std::endl;


int main(int argc, char **argv)
{
    SubsetterCommands cmd;

    pagoda::initialize(&argc, &argv);
    cmd.parse(argc,argv);

    if (0 == pagoda::me) {
        cout << cmd.get_usage() << endl;
        cout << pagoda::vec_to_string(cmd.get_input_filenames()) << endl;
        cout << cmd.get_output_filename() << endl;
        cout << pagoda::vec_to_string(cmd.get_slices()) << endl;
        cout << cmd.get_box() << endl;
    }

    pagoda::finalize();

    return EXIT_SUCCESS;
}
