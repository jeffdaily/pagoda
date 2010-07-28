#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "PgboCommands.H"
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
int F77_DUMMY_MAIN() { return 1; }
#endif

//#define READ_ALL
//#define READ_RECORD
//#define READ_NONBLOCKING

int main(int argc, char **argv)
{
    PgboCommands cmd;
    string op_type;
    Dataset *dataset;
    vector<Variable*> ds_vars;
    vector<Dimension*> ds_dims;
    MaskMap *ds_masks;
    Dataset *operand;
    vector<Variable*> op_vars;
    vector<Dimension*> op_dims;
    MaskMap *op_masks;
    vector<Variable*>::iterator var_it;
    FileWriter *writer;
    vector<Grid*> grids;
    Grid *grid;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.get_help()) {
            pagoda::print_zero(cmd.get_usage());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        dataset = cmd.get_dataset();
        operand = cmd.get_operand();
        op_type = cmd.get_operator();

        ds_vars = cmd.get_variables(dataset);
        op_vars = cmd.get_variables(operand);
        ds_dims = cmd.get_dimensions(dataset);
        op_dims = cmd.get_dimensions(operand);

        // okay, we assume dataset and operand have the same grid
        grids = dataset->get_grids();
        if (grids.empty()) {
            pagoda::print_zero("no grid found\n");
        } else {
            grid = grids[0];
        }
        
        // create masks for dataset and operand
        ds_masks = new MaskMap(dataset);
        ds_masks->modify(cmd.get_slices());
        ds_masks->modify(cmd.get_boxes(), grid);
        op_masks = new MaskMap(operand);
        op_masks->modify(cmd.get_slices());
        op_masks->modify(cmd.get_boxes(), grid);

        // create the output writer
        writer = cmd.get_output();
        writer->write_atts(cmd.get_attributes(dataset));
        writer->def_dims(ds_dims);
        writer->def_vars(ds_vars);

        // handle one variable at a time
        for (var_it=ds_vars.begin(); var_it!=ds_vars.end(); ++var_it) {
            Variable *ds_var = *var_it;
            Variable *op_var = operand->get_var(ds_var->get_name());
            if (!op_var
                    || grid->is_coordinate(ds_var)
                    || grid->is_topology(ds_var)) {
                // no corresponding variable in the operand dataset,
                // or we have a coordinate or topology variable; write
                if (ds_var->has_record()) {
                    Array *array = NULL; // reuse allocated array each record
                    for (int64_t rec=0,limit=ds_var->get_nrec();
                            rec<limit; ++rec) {
                        array = ds_var->read(rec, array);
                        writer->write(array, ds_var->get_name(), rec);
                    }
                    delete array;
                } else {
                    Array *array = ds_var->read();
                    writer->write(array, ds_var->get_name());
                    delete array;
                }
            } else if (ds_var->has_record() && op_var->has_record()) {
                // both dataset and operand variables have a record dimension
                // operate one record at a time
                Array *ds_array = NULL; // reuse allocated array each record
                Array *op_array = NULL; // reuse allocated array each record
                for (int64_t rec=0,limit=ds_var->get_nrec(); rec<limit; ++rec) {
                    ds_array = ds_var->read(rec, ds_array);
                    op_array = op_var->read(rec, op_array);
                    if (op_type == "+") {
                        ds_array = ds_array->iadd(op_array);
                    } else if (op_type == "-") {
                        ds_array = ds_array->isub(op_array);
                    } else if (op_type == "*") {
                        ds_array = ds_array->imul(op_array);
                    } else if (op_type == "/") {
                        ds_array = ds_array->idiv(op_array);
                    } else {
                        ERR("bad op_type"); // shouldn't happen, but still...
                    }
                    writer->write(ds_array, ds_var->get_name(), rec);
                }
                delete ds_array;
                delete op_array;
            } else if (ds_var->has_record()) {
                // only the dataset variable has a record dimension
            } else if (op_var->has_record()) {
                // only the operand variable has a record dimension (how??)
            } else {
                // neither variable has a record dimension
            }
        }

        // clean up
        delete dataset;
        delete operand;
        delete ds_masks;
        delete op_masks;
        delete writer;

        pagoda::finalize();

    } catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}
