#include <string>

#include "FileFormat.H"

using std::string;

string pagoda::file_format_to_string(FileFormat format)
{
    if (FF_NETCDF4 == format) {
        return "NETCDF4";
    }
    else if (FF_NETCDF4_CLASSIC == format) {
        return "NETCDF4_CLASSIC";
    }
    else if (FF_CDF1 == format) {
        return "CDF1";
    }
    else if (FF_CDF2 == format) {
        return "CDF2";
    }
    else if (FF_CDF5 == format) {
        return "CDF5";
    }
    else {
        return "UNKNOWN";
    }
}
