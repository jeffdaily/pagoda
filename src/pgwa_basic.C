#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "NotImplementedException.H"
#include "PgwaCommands.H"
#include "Print.H"
#include "ScalarArray.H"
#include "Util.H"
#include "Validator.H"
#include "Variable.H"

using std::exception;
using std::string;
using std::map;
using std::vector;

#define NO_OUTPUT 1

#ifdef F77_DUMMY_MAIN
#   ifdef __cplusplus
extern "C"
#   endif
int F77_DUMMY_MAIN()
{
    return 1;
}
#endif


void pgwa_blocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgwaCommands &cmd);


int main(int argc, char **argv)
{
    PgwaCommands cmd;
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    FileWriter *writer = NULL;
    string op;

    try {
        pagoda::initialize(&argc, &argv);

        Variable::promote_to_float = true;

#if NO_OUTPUT
        // it was easier to fake argv rather than remove the output requirement
        char **argv_copy = new char*[3];
        if (argc <= 1) {
            throw CommandException("missing input file");
        }
        argv_copy[0] = argv[0];
        argv_copy[1] = argv[1];
        argv_copy[2] = "dummy";
        argc = 3;
        cmd.parse(argc,argv_copy);
#else
        cmd.parse(argc,argv);
#endif
#if NO_OUTPUT
        vector<Dimension*> dims;
        vector<Attribute*> atts;
        cmd.get_inputs(dataset, vars, dims, atts);
#else
        cmd.get_inputs_and_outputs(dataset, vars, writer);
#endif
        op = cmd.get_operator();

        if (cmd.is_nonblocking()) {
            if (cmd.is_reading_all_records()) {
                //pgra_nonblocking_allrec(dataset, vars, writer, op, cmd);
            } else {
                if (cmd.get_number_of_groups() > 1) {
                    //pgra_nonblocking_groups(dataset, vars, writer, op, cmd);
                } else {
                    //pgra_nonblocking(dataset, vars, writer, op, cmd);
                }
            }
        } else {
            if (cmd.get_number_of_groups() > 1) {
                //pgra_blocking_groups(dataset, vars, writer, op, cmd);
            } else {
                pgwa_blocking(dataset, vars, writer, op, cmd);
            }
        }

        // clean up
        if (dataset) {
            delete dataset;
        }
        if (writer) {
            delete writer;
        }

        pagoda::finalize();
    }
    catch (CommandException &ex) {
        if (dataset) {
            delete dataset;
        }
        if (writer) {
            delete writer;
        }
        pagoda::println_zero(ex.what());
        pagoda::finalize();
        return EXIT_FAILURE;
    }
    catch (PagodaException &ex) {
        pagoda::abort(ex.what());
    }
    catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}


void pgwa_blocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgwaCommands &cmd)
{
    vector<Variable*>::const_iterator var_it;
    set<string> reduced_dims = cmd.get_reduced_dimensions();

    // read each variable in order
    // for record variables, read one record at a time
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        if (cmd.is_verbose()) {
            pagoda::println_zero("processing variable: " + var->get_name());
        }
        if (var->has_record() && var->get_nrec() > 0) {
            int64_t nrec = var->get_nrec();
            Array *result = NULL;
            Array *array = NULL; // reuse allocated array each record

            array = var->read(0, array);
            if (cmd.is_verbose()) {
                pagoda::println_zero("\tfinished reading record 0");
            }

            if (reduced_dims.empty() && op == OP_AVG) {
                result = array->reduce_add();
            } else {
                throw NotImplementedException("TODO");
            }

            // read the rest of the records
            for (int64_t rec=1; rec<nrec; ++rec) {
                array = var->read(rec, array);
                if (cmd.is_verbose()) {
                    pagoda::println_zero("\tfinished reading record "
                            + pagoda::to_string(rec));
                }

                result->iadd(array->reduce_add());
            }

            ScalarArray tally(result->get_type());
            tally.fill_value(nrec);
            result->idiv(&tally);

#if NO_OUTPUT
            pagoda::print_zero(var->get_name() + " = ");
            result->dump();
#else
            writer->write(result, var->get_name());
            if (cmd.is_verbose()) {
                pagoda::println_zero("\tfinished writing");
            }
#endif
            delete result;
            delete array;
        }
        else {
            Array *array = var->read();
            Array *result = NULL;
            ASSERT(NULL != array);
            if (reduced_dims.empty() && op == OP_AVG) {
                result = array->reduce_add();
                ScalarArray denominator(result->get_type());
                denominator.fill_value(array->get_size());
                result->idiv(&denominator);
            }
            else {
                throw NotImplementedException("TODO");
            }
#if NO_OUTPUT
            pagoda::print_zero(var->get_name() + " = ");
            result->dump();
#else
            writer->write(result, var->get_name());
#endif
            delete array;
            delete result;
        }
    }
}

