#ifndef NETCDFFILEWRITER_H_
#define NETCDFFILEWRITER_H_

#include <string>
#include <map>
#include <vector>

#include "FileWriter.H"

class Dataset;
class NetcdfVariable;
class Variable;

using std::string;
using std::map;
using std::vector;


/**
 * Writes a Dataset to a netcdf-3 file using pnetcdf.
 */
class NetcdfFileWriter : public FileWriter
{
    public:
        NetcdfFileWriter(const string &filename);
        virtual ~NetcdfFileWriter();

        virtual void def_dim(Dimension *dim);

        virtual void def_var(Variable *var);

        virtual void copy_att(Attribute *att, const string &name=string(""));

        virtual void write(int handle, const string &name, int record=-1);

        virtual ostream& print(ostream &os) const;

    protected:
        int get_dim_id(const string &name);
        int get_var_id(const string &name);

        void def_check();
        void maybe_enddef();

        void copy_att_id(Attribute *attr, int id);
        void copy_atts_id(const vector<Attribute*> &atts, int id);
        void write(int handle, int id, int record=-1);

        bool is_in_define_mode;
        string filename;
        int ncid;
        map<string,int> dim_id;
        map<string,int> var_id;
};

#endif // NETCDFFILEWRITER_H_
