#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <iostream>

#include <ga.h>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include "FileWriter.H"
#include "Pack.H"
#include "SubsetterCommands.H"
#include "SubsetterException.H"
#include "Util.H"
#include "Variable.H"

static void subset(Variable *var, FileWriter *writer);
static void subset_record(Variable *var, FileWriter *writer);

#ifdef F77_DUMMY_MAIN
#  ifdef __cplusplus
     extern "C"
#  endif
   int F77_DUMMY_MAIN() { return 1; }
#endif


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    TRACER("AFTER INITS\n");

    SubsetterCommands cmd;
    Dataset *dataset;
    Aggregation *agg;
    vector<Variable*> vars;
    FileWriter *writer;

    TRACER("cmd line parsing BEGIN\n");
    try {
        cmd.parse(argc,argv);
    }
    catch (SubsetterException &ex) {
        if (ME == 0) {
            std::cout << ex.what() << std::endl;
        }
        exit(EXIT_FAILURE);
    }
    TRACER("cmd line parsing END\n");

    const vector<string> &infiles = cmd.get_intput_filenames();
    if (cmd.get_join_name().empty()) {
        dataset = agg = new AggregationUnion;
    } else {
        dataset = agg = new AggregationJoinExisting(cmd.get_join_name());
    }
    for (size_t i=0,limit=infiles.size(); i<limit; ++i) {
        TRACER1("adding %s\n", infiles[i].c_str());
        agg->add(Dataset::open(infiles[i]));
    }

    Util::calculate_required_memory(dataset->get_vars());

    dataset->create_masks();
    dataset->adjust_masks(cmd.get_slices());
    dataset->adjust_masks(cmd.get_box());

    vars = dataset->get_vars();
    writer = FileWriter::create(cmd.get_output_filename());
    writer->copy_atts(dataset->get_atts());
    writer->def_dims(dataset->get_dims());
    writer->def_vars(vars);
    for (vector<Variable*>::iterator it=vars.begin(); it!=vars.end(); ++it) {
        Variable *var = *it;
        if (var->has_record() && var->num_dims() > 1) {
            subset_record(var, writer);
        } else {
            subset(var, writer);
        }
    }

    delete writer; // also closes file
    TRACER("after delete writer\n");
    delete dataset;
    TRACER("after delete dataset\n");

    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
}


// subset and write var
void subset(Variable *var, FileWriter *writer)
{
    const string name = var->get_name();
    TRACER1("SubsetterMain::subset %s BEGIN\n", name.c_str());

    var->read();
    var->reindex(); // noop if not ConnectivityVariable

    if (var->num_cleared_masks() > 0) {
        int ga_out;
        size_t ndim = var->num_dims();
        int ga_masks[ndim];
        vector<int64_t> shape_out = var->get_sizes(true);
        int matype = var->get_type().as_mt();

        // HACK for now since all masks are distributed by default
        for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
            vector<Dimension*> dims = var->get_dims();
            DistributedMask *mask;
            mask = dynamic_cast<DistributedMask*>(dims.at(dimidx)->get_mask());
            ga_masks[dimidx] = mask->get_handle();
            TRACER2("mask handle %zd=%d\n", dimidx, ga_masks[dimidx]);
        }

        TRACER("before ga_out (pack_dst) create\n");
        ga_out = NGA_Create64(matype, ndim, &shape_out[0], "pack_dst", NULL);
        pack(var->get_handle(), ga_out, ga_masks);
        writer->write(ga_out, name);
        GA_Destroy(ga_out);
    } else {
        // no masks, so a direct copy
        writer->write(var->get_handle(), name);
    }

    var->release_handle();
    TRACER1("SubsetterMain::subset %s END\n", name.c_str());
}


// subset and write record var
// TODO what if there is no mask (num_cleared_masks)??  Need if/else block
void subset_record(Variable *var, FileWriter *writer)
{
    const string name = var->get_name();
    TRACER1("SubsetterMain::subset_record %s BEGIN\n", name.c_str());
    vector<Dimension*> dims = var->get_dims();
    size_t ndim = dims.size();
    size_t ndim_1 = ndim - 1;
    int *mask_rec;
    int ga_masks[ndim];
    vector<int64_t> shape_out = var->get_sizes(true);
    int ga_out;
    int matype = var->get_type().as_mt();

    if (dims[0]->get_mask()) {
        mask_rec = dims[0]->get_mask()->get_data();
    } else {
        int ONE = 1;
        mask_rec = new int[dims[0]->get_size()];
        std::fill(mask_rec, mask_rec+dims[0]->get_size(), ONE);
    }

    // HACK for now since all masks are distributed by default
    for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
        DistributedMask *mask;
        mask = dynamic_cast<DistributedMask*>(dims.at(dimidx)->get_mask());
        ga_masks[dimidx] = mask->get_handle();
        TRACER2("mask handle %zd=%d\n", dimidx, ga_masks[dimidx]);
    }

    ga_out = NGA_Create64(matype, ndim_1, &shape_out[1], "pack_dst", NULL);

    for (int64_t record=0,nrec=dims[0]->get_size(),packed_recidx=0; record<nrec; ++record) {
        if (mask_rec[record] != 0) {
            var->set_record_index(record);
            var->read();
            pack(var->get_handle(), ga_out, ga_masks+1);
            writer->write(ga_out, name, packed_recidx++);
        }
    }

    GA_Destroy(ga_out);
    delete [] mask_rec;
    var->release_handle();
    TRACER1("SubsetterMain::subset_record %s END\n", name.c_str());
}
