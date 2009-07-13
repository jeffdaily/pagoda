#ifndef NETCDFFILEWRITER_H_
#define NETCDFFILEWRITER_H_

#include <string>
    using std::string;

class ConnectivityVariable;
class Dataset;
class NetcdfVariable;
class Variable;


/**
 * Writes a Dataset to a netcdf-3 file.
 */
class NetcdfFileWriter
{
    public:
        static void write(const string &filename, Dataset *dataset);

    protected:
        static void copy_att(Attribute *attr, int ncid, int varid);
        static void copy_atts(const vector<Attribute*> &atts, int ncid, int varid);
        static void copy_var(ConnectivityVariable *var_in,
                NetcdfVariable *var_out);
        static void copy_var(Variable *var_in, NetcdfVariable *var_out);
        static void copy_record_var(Variable *var_in, NetcdfVariable *var_out);
};

#endif // NETCDFFILEWRITER_H_
