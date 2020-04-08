#include "parallel_core.h"

namespace apl
{

    double parallel_engine::start_time;

    parallel_engine::parallel_engine(int* argc, char*** argv)
    {
        init_library(argc, argv);
    }

    parallel_engine::~parallel_engine()
    {
        finalize_library();
    }

    void parallel_engine::init_library(int* argc, char*** argv)
    {
        int flag;
        MPI_Initialized(&flag);
        if (!flag)
        {
            MPI_Init(argc, argv);
            start_time = MPI_Wtime();
        }
    }

    void parallel_engine::finalize_library()
    {
        int flag;
        MPI_Initialized(&flag);
        if (flag)
            MPI_Finalize();
    }

    double parallel_engine::get_start_time()
    { return start_time; }

}