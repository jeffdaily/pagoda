#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "SubsetterCommands.H"
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

//#define READ_ALL
#define READ_RECORD
//#define READ_NONBLOCKING

int main(int argc, char **argv)
{
    SubsetterCommands cmd;
    Dataset *dataset;
    vector<Variable*> vars;
    vector<Variable*>::iterator var_it;
    vector<Dimension*> dims;
    FileWriter *writer;
    vector<Grid*> grids;
    Grid *grid;
    MaskMap *masks;
#if defined(READ_NONBLOCKING)
    map<string,Array*> arrays;
    map<string,Array*>::iterator array;
#endif

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_helping()) {
            pagoda::print_zero(cmd.get_usage());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        dataset = cmd.get_dataset();
        vars = cmd.get_variables(dataset);
        dims = cmd.get_dimensions(dataset);

        grids = dataset->get_grids();
        if (grids.empty()) {
            pagoda::print_zero("no grid found\n");
        }
        else {
            grid = grids[0];
        }

        masks = new MaskMap(dataset);
        masks->modify(cmd.get_slices());
        masks->modify(cmd.get_boxes(), grid);

        writer = cmd.get_output();
        writer->write_atts(cmd.get_attributes(dataset));
        writer->def_dims(dims);
        writer->def_vars(vars);

        // read/subset and write each variable...
#ifdef READ_ALL
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            Array *array = var->read();
            writer->write(array, var->get_name());
            delete array;
        }
#elif defined(READ_RECORD)
        // write all non-record variables first
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            if (var->has_record()) {
                Array *array = NULL; // reuse allocated array each record
                for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec) {
                    array = var->read(rec, array);
                    writer->write(array, var->get_name(), rec);
                }
                delete array;
            }
            else {
                Array *array = var->read();
                writer->write(array, var->get_name());
                delete array;
            }
        }
#elif defined(READ_NONBLOCKING)
        // read all non-record variables first, non-blocking
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            if (!var->has_record()) {
                arrays[var->get_name()] = var->iread();
            }
        }
        dataset->wait();
        // write all non-record variables first
        /** @todo implement non-blocking read */
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            array = arrays.find(var->get_name());
            if (array != arrays.end()) {
                writer->write(array->second,array->first);
            }
        }
#endif

        // clean up
        delete dataset;
        delete masks;
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
