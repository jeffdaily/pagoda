#ifndef COMMANDLINEPARSER_H_
#define COMMANDLINEPARSER_H_

#include <string>
#include <vector>

using std::string;
using std::vector;

#include "LatLonBox.H"


/**
 * Common command-line parsing capabilities are found here.
 */
class CommandLineParser
{
    public:
        CommandLineParser(int argc, char **argv);
        CommandLineParser(const CommandLineParser &that);
        virtual ~CommandLineParser();

        vector<string> get_intput_filenames() const;
        string get_output_filename() const;
        bool has_box() const;
        LatLonBox get_box() const;
        //DimSliceMMap get_slices() const;

    private:
        vector<string> input_filenames;
        string output_filename;
        bool _has_box;
        LatLonBox box;

        
};

#endif // COMMANDLINEPARSER_H_
