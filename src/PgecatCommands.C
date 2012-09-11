#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "GenericCommands.H"
#include "PgecatCommands.H"


PgecatCommands::PgecatCommands()
    :   GenericCommands()
    ,   unlimited_name("record")
{
    init();
}


PgecatCommands::PgecatCommands(int argc, char **argv)
    :   GenericCommands()
    ,   unlimited_name("record")
{
    init();
    parse(argc, argv);
}


PgecatCommands::~PgecatCommands()
{
}


void PgecatCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc, argv);

    if (parser.count(CommandLineOption::UNLIMITED_DIMENSION_NAME)) {
        unlimited_name = 
            parser.get_argument(CommandLineOption::UNLIMITED_DIMENSION_NAME);
    }
}


string PgecatCommands::get_unlimited_name() const
{
    return unlimited_name;
}


void PgecatCommands::init()
{
    parser.push_back(CommandLineOption::UNLIMITED_DIMENSION_NAME);
}
