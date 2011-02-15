#ifndef FILEFORMAT_H_
#define FILEFORMAT_H_

#include <string>

using std::string;

enum FileFormat {
    FF_NETCDF4,
    FF_NETCDF4_CLASSIC,
    FF_CDF1,
    FF_CDF2,
    FF_CDF5,
    FF_UNKNOWN,
};

namespace pagoda {
    string file_format_to_string(FileFormat format);
};

#endif /* FILEFORMAT_H_ */
