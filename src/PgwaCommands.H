#ifndef PGWACOMMANDS_H_
#define PGWACOMMANDS_H_

#include <set>

#include "GenericCommands.H"

using std::set;


/**
 * All command-line parsing for the subsetter.
 */
class PgwaCommands : public GenericCommands
{
    public:
        PgwaCommands();
        PgwaCommands(int argc, char **argv);
        virtual ~PgwaCommands();

        virtual void parse(int argc, char **argv);

        string get_operator() const;

    protected:
        void init();

        static vector<string> VALID;

        string op_type;
        string mask_value;
        string mask_variable;
        string mask_comparator;
        set<string> dimensions;
};

#endif // PGWACOMMANDS_H_
