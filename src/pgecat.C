#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdlib>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "Dataset.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "Print.H"
#include "PgecatCommands.H"
#include "Util.H"
#include "Variable.H"

using std::exception;
using std::string;
using std::map;
using std::vector;


#ifdef F77_DUMMY_MAIN
#   ifdef __cplusplus
extern "C"
#   endif
int F77_DUMMY_MAIN()
{
    return 1;
}
#endif


void pgecat_blocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);
void pgecat_blocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);
void pgecat_nonblocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);
void pgecat_nonblocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);


int main(int argc, char **argv)
{
    PgecatCommands cmd;
    Dataset *dataset;
    vector<Variable*> vars;
    FileWriter *writer;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);
        cmd.get_inputs_and_outputs(dataset, vars, writer);

        if (cmd.is_reading_all_records()) {
            if (cmd.is_nonblocking()) {
                pgecat_nonblocking_read_all(dataset, vars, writer);
            } else {
                pgecat_blocking_read_all(dataset, vars, writer);
            }
        } else {
            if (cmd.is_nonblocking()) {
                pgecat_nonblocking(dataset, vars, writer);
            } else {
                pgecat_blocking(dataset, vars, writer);
            }
        }

        // clean up
        delete dataset;
        delete writer;

        pagoda::finalize();

    }
    catch (CommandException &ex) {
        pagoda::println_zero(ex.what());
        pagoda::finalize();
        return EXIT_FAILURE;
    }
    catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}


void pgecat_blocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
}


void pgecat_blocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
}


void pgecat_nonblocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
}


void pgecat_nonblocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
}
