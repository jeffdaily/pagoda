#include "SubsetterCommands.H"
#include "SubsetterException.H"
#include "Util.H"


SubsetterCommands::SubsetterCommands()
    :   input_filenames()
    ,   output_filename("")
    ,   slices()
    ,   _has_box(false)
    ,   box()
{
}


SubsetterCommands::SubsetterCommands(int argc, char **argv)
    :   input_filenames()
    ,   output_filename("")
    ,   slices()
    ,   _has_box(false)
    ,   box()
{
}


SubsetterCommands::SubsetterCommands(const SubsetterCommands &that)
    :   input_filenames(that.input_filenames)
    ,   output_filename(that.output_filename)
    ,   slices(that.slices)
    ,   _has_box(that._has_box)
    ,   box(that.box)
{
}


SubsetterCommands::~SubsetterCommands()
{
}


void SubsetterCommands::parse(int argc, char **argv)
{
    int c;
    opterr = 0;
    while ((c = getopt(argc, argv, "hb:d:")) != -1) {
        switch (c) {
            case 'h':
                throw SubsetterException(get_usage());
            case 'b':
                _has_box = true;
                box = LatLonBox(optarg);
                // HACK - we know our stuff is in radians
                // so convert the degrees from cmd line to radians
                box.scale(RAD_PER_DEG);
                break;
            case 'd':
                try {
                    slices.push_back(DimSlice(optarg));
                } catch (RangeException &ex) {
                    ERR("Bad dimension slice");
                }
                break;
            default:
                ostringstream os;
                os << "ERROR: Unrecognized argument '" << c << "'" << endl;
                os << get_usage() << endl;
                throw SubsetterException(os.str());
        }
    }

    if (optind == argc) {
        ostringstream os;
        os << "ERROR: Input and output file arguments required" << endl;
        os << get_usage() << endl;
        throw SubsetterException(os.str());
    } else if (optind+1 == argc) {
        ostringstream os;
        os << "ERROR: Output file argument required" << endl;
        os << get_usage() << endl;
        throw SubsetterException(os.str());
    }

    while (optind < argc-1) {
        input_filenames.push_back(argv[optind]);
        ++optind;
    }
    output_filename = argv[optind];
}


const string& SubsetterCommands::get_usage() const
{
    static string usage = ""
        "Usage: subsetter [options] infile1 [infile2 ...] output_filename\n"
        "\n"
        "Generic options:\n"
        "-h                                 produce this usage statement\n"
        "-b box north,south,east,west       limits in degrees\n"
        "-d dimension,start[,stop[,step]]   as integer indices\n"
        ;
    return usage;
}


const vector<string>& SubsetterCommands::get_intput_filenames() const
{
    return input_filenames;
}


const string& SubsetterCommands::get_output_filename() const
{
    return output_filename;
}


bool SubsetterCommands::has_box() const
{
    return _has_box;
}


const LatLonBox& SubsetterCommands::get_box() const
{
    return box;
}


const vector<DimSlice>& SubsetterCommands::get_slices() const
{
    return slices;
}

