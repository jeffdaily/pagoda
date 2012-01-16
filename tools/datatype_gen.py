#!/usr/bin/env python

# we want them generated in this order
types_keys=[
        "DataType::CHAR",
        "DataType::SHORT",
        "DataType::INT",
        "DataType::LONG",
        "DataType::LONGLONG",
        "DataType::FLOAT",
        "DataType::DOUBLE",
        "DataType::LONGDOUBLE",
        "DataType::UCHAR",
        "DataType::USHORT",
        "DataType::UINT",
        "DataType::ULONG",
        "DataType::ULONGLONG",
        "DataType::SCHAR",
        ]
types={
        "DataType::CHAR":      "char",
        "DataType::SHORT":     "short",
        "DataType::INT":       "int",
        "DataType::LONG":      "long",
        "DataType::LONGLONG":  "long long",
        "DataType::FLOAT":     "float",
        "DataType::DOUBLE":    "double",
        "DataType::LONGDOUBLE":"long double",
        "DataType::UCHAR":     "unsigned char",
        "DataType::USHORT":    "unsigned short",
        "DataType::UINT":      "unsigned int",
        "DataType::ULONG":     "unsigned long",
        "DataType::ULONGLONG": "unsigned long long",
        "DataType::SCHAR":     "signed char",
        }


f = open("DataType.def", "w")
for type1 in types_keys:
    f.write("DATATYPE_EXPAND(%s,%s)\n" % (
        type1, types[type1]))
f.write("#undef DATATYPE_EXPAND\n")
f.close()

f = open("DataType2.def", "w")
for type1 in types_keys:
    for type2 in types_keys:
        f.write("DATATYPE_EXPAND(%s,%s,%s,%s)\n" % (
            type1, types[type1], type2, types[type2]))
f.write("#undef DATATYPE_EXPAND\n")
f.close()

#f = open("DataType3.def", "w")
#for type1 in types_keys:
#    for type2 in types_keys:
#        for type3 in types_keys:
#            f.write("DATATYPE_EXPAND(%s,%s,%s,%s,%s,%s)\n" % (
#                type1, types[type1], type2, types[type2], type3, types[type3]))
#f.write("#undef DATATYPE_EXPAND\n")
#f.close()
