#ifndef PGRSUBCOMMANDS_H_
#define PGRSUBCOMMANDS_H_

#include <string>
#include <vector>

#include "GenericCommands.H"

using std::string;
using std::vector;

class Dataset;
class Dimension;
class Variable;


/**
 * All command-line parsing for the subsetter.
 */
class PgrsubCommands : public GenericCommands
{
    public:
        PgrsubCommands();
        PgrsubCommands(int argc, char **argv);
        virtual ~PgrsubCommands();

        virtual void parse(int argc, char **argv);

        virtual Dataset* get_dataset();
        virtual vector<Variable*> get_variables(Dataset *dataset);
        virtual vector<Dimension*> get_dimensions(Dataset *dataset);
        virtual vector<string> get_dimension_masks() const;
        virtual string get_output_filename_with_index(int64_t index) const;
        virtual FileWriter* get_output_at(int64_t index) const;

        virtual string get_join_name() const;
        virtual bool is_alphabetizing() const;

    protected:
        void init();

        string join_name;
        bool alphabetize;
        vector<string> dimension_masks;
};

#endif // PGRSUBCOMMANDS_H_
