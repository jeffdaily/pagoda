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
        virtual void close();

        virtual void def_dim(const string &name, int64_t size);

        virtual void def_var(const string &name,
                const vector<string> &dims,
                const DataType &type,
                const vector<Attribute*> &atts);

        virtual void copy_att(Attribute *att, const string &name=string(""));

        virtual void write(Array *array, const string &name);
        virtual void write(Array *array, const string &name,
                int64_t record);
        virtual void write(Array *array, const string &name,
                const vector<int64_t> &start);

        virtual ostream& print(ostream &os) const;

    protected:
        int get_dim_id(const string &name) const;
        int64_t get_dim_size(const string &name) const;
        int get_var_id(const string &name) const;
        vector<int> get_var_dims(const string &name) const;
        vector<int64_t> get_var_shape(const string &name) const;

        void def_check() const;
        void maybe_enddef();

        void copy_att_id(Attribute *attr, int varid);
        void copy_atts_id(const vector<Attribute*> &atts, int varid);
        void write(int handle, int id, int record=-1);

        bool is_in_define_mode;
        string filename;
        int ncid;

        map<string,int>     dim_id;
        map<string,int64_t> dim_size;

        map<string,int>              var_id;
        map<string,vector<int> >     var_dims;
        map<string,vector<int64_t> > var_shape;

        bool open;
};

#endif // NETCDFFILEWRITER_H_
