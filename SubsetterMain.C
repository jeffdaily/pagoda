#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

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
#include "Timing.H"
#include "Variable.H"

using std::cout;
using std::endl;

static void subset(Variable *var, FileWriter *writer, map<int,int> sum_map);
static void subset_record(Variable *var, FileWriter *writer, map<int,int> sum_map);

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
    vector<Dimension*> dims;
    FileWriter *writer;
    map<int,int> sum_map;

    TRACER("cmd line parsing BEGIN\n");
    try {
        cmd.parse(argc,argv);
    }
    catch (SubsetterException &ex) {
        if (ME == 0) {
            cout << ex.what() << endl;
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
    dataset->decorate();

    Util::calculate_required_memory(dataset->get_vars());

    dataset->create_masks();
    dataset->adjust_masks(cmd.get_slices());
    dataset->adjust_masks(cmd.get_box());

    vars = dataset->get_vars();
    dims = dataset->get_dims();

    TRACER("precompute partial sums BEGIN\n");
    // TODO It was easier to simply create partial sums for all masks.
    // It might be wiser to create partial sums for only those dimensions of
    // variables which need subsetting.
    for (vector<Dimension*>::iterator dimit=dims.begin();
            dimit!=dims.end(); ++dimit) {
        Dimension *dim = *dimit;
        DistributedMask *mask;
        mask = dynamic_cast<DistributedMask*>(dim->get_mask());
        if (mask) {
            int g_mask = mask->get_handle();
            if (sum_map.find(g_mask) == sum_map.end()) {
                TRACER1("Creating partial sum for %s\n",
                        mask->get_name().c_str());
                sum_map[g_mask] = GA_Duplicate(g_mask, "maskcopy");
                partial_sum(g_mask, sum_map[g_mask], 0);
            }
        }
    }
    TRACER("precompute partial sums END\n");

    writer = FileWriter::create(cmd.get_output_filename());
    writer->copy_atts(dataset->get_atts());
    writer->def_dims(dataset->get_dims());
    writer->def_vars(vars);
    for (vector<Variable*>::iterator it=vars.begin(); it!=vars.end(); ++it) {
        Variable *var = *it;
        if (var->has_record() && var->num_dims() > 1) {
            subset_record(var, writer, sum_map);
        } else {
            subset(var, writer, sum_map);
        }
    }

    delete writer; // also closes file
    TRACER("after delete writer\n");
    delete dataset;
    TRACER("after delete dataset\n");

#ifdef TRACE
    if (0 == ME) {
        GA_Print_stats();
    }
#endif

    GA_Terminate();
    MPI_Finalize();
    return EXIT_SUCCESS;
}


// subset and write var
void subset(Variable *var, FileWriter *writer, map<int,int> sum_map)
{
    const string name = var->get_name();
    TRACER1("SubsetterMain::subset %s BEGIN\n", name.c_str());

    var->read();
    var->reindex(); // noop if not ConnectivityVariable

    if (var->needs_subset()) {
        int ga_out;
        size_t ndim = var->num_dims();
        int ga_masks[ndim];
        int ga_masksums[ndim];
        vector<int64_t> shape_out = var->get_sizes(true);
        int matype = var->get_type();

        // HACK for now since all masks are distributed by default
        for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
            vector<Dimension*> dims = var->get_dims();
            DistributedMask *mask;
            mask = dynamic_cast<DistributedMask*>(dims.at(dimidx)->get_mask());
            if (!mask) {
                GA_Error("missing DistributedMask\n", 0);
            }
            ga_masks[dimidx] = mask->get_handle();
            if (sum_map.find(ga_masks[dimidx]) == sum_map.end()) {
                GA_Error("missing partial sum", ga_masks[dimidx]);
            }
            ga_masksums[dimidx] = sum_map[ga_masks[dimidx]];
            TRACER4("ga_masks[%zd]=%d,ga_masksums[%zd]=%d\n",
                    dimidx, ga_masks[dimidx], dimidx, ga_masksums[dimidx]);
        }

        TRACER("before ga_out (pack_dst) create\n");
        ga_out = NGA_Create64(matype, ndim, &shape_out[0], "pack_dst", NULL);
        pack(var->get_handle(), ga_out, ga_masks, ga_masksums);
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
void subset_record(Variable *var, FileWriter *writer, map<int,int> sum_map)
{
    const string name = var->get_name();
    TRACER1("SubsetterMain::subset_record %s BEGIN\n", name.c_str());
    vector<Dimension*> dims = var->get_dims();
    size_t ndim = dims.size();
    TRACER1("ndim=%zd\n", ndim);
    size_t ndim_1 = ndim - 1;
    vector<int> mask_rec;
    int ga_masks[ndim-1];
    int ga_masksums[ndim-1];
    TRACER("Before getting shape_out\n");
    vector<int64_t> shape_out = var->get_sizes(true);
    int ga_out;
    int matype = var->get_type();

    TRACER("Before record mask retrieval\n");
    if (dims[0]->get_mask()) {
        dims[0]->get_mask()->get_data(mask_rec);
    } else {
        mask_rec.assign(dims[0]->get_size(), 1);
    }

    TRACER("Before 'hack' loop\n");
    // HACK for now since all masks are distributed by default
    for (size_t dimidx=1; dimidx<ndim; ++dimidx) {
        DistributedMask *mask;
        mask = dynamic_cast<DistributedMask*>(dims.at(dimidx)->get_mask());
        if (!mask) {
            GA_Error("missing DistributedMask\n", 0);
        }
        ga_masks[dimidx-1] = mask->get_handle();
        if (sum_map.find(ga_masks[dimidx-1]) == sum_map.end()) {
            GA_Error("missing partial sum", ga_masks[dimidx-1]);
        }
        ga_masksums[dimidx-1] = sum_map[ga_masks[dimidx-1]];
        TRACER4("ga_masks[%zd]=%d,ga_masksums[%zd]=%d\n",
                dimidx, ga_masks[dimidx-1], dimidx, ga_masksums[dimidx-1]);
    }

    ga_out = NGA_Create64(matype, ndim_1, &shape_out[1], "pack_dst", NULL);

    for (int64_t record=0,nrec=dims[0]->get_size(),packed_recidx=0; record<nrec; ++record) {
        if (mask_rec[record] != 0) {
            var->set_record_index(record);
            var->read();
            pack(var->get_handle(), ga_out, ga_masks, ga_masksums);
            writer->write(ga_out, name, packed_recidx++);
        }
    }

    GA_Destroy(ga_out);
    var->release_handle();
    TRACER1("SubsetterMain::subset_record %s END\n", name.c_str());
#ifdef GATHER_TIMING
    if (0 == ME) {
        cout << Timing::get_stats_calls() << endl;
        cout << endl;
        cout << endl;
        cout << Timing::get_stats_total_time() << endl;
    }
#endif
}
